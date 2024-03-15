--[[
  实现 socketchannel 的需求:

  请求回应模式是和外部服务交互时所用到的最常用模式之一。通常的协议设计方式有两种。

  1. 每个请求包对应一个回应包，由 TCP 协议保证时序。redis 的协议就是一个典型。
  每个 redis 请求都必须有一个回应，但不必收到回应才可以发送下一个请求。

  2. 发起每个请求时带一个唯一 session 标识，在发送回应时，带上这个标识。这样设计可以不要求每个请求都一定要有回应，
  且不必遵循先提出的请求先回应的时序。MongoDB 的通讯协议就是这样设计的。

  对于第一种模式，用 skynet 的 Socket API 很容易实现，但如果在一个 coroutine 中读写一个 socket 的话，
  由于读的过程是阻塞的，这会导致吞吐量下降（前一个回应没有收到时，无法发送下一个请求）。

  对于第二种模式，需要用 skynet.fork 开启一个新线程来收取回应包，并自行和请求对应起来，实现比较繁琐。

--]]

local skynet = require "skynet"
local socket = require "skynet.socket"
local socketdriver = require "skynet.socketdriver"

-- channel support auto reconnect , and capture socket error in request/response transaction
-- channel 支持自动重连, 并且在处理 request/response 事物的时候捕获 socket 错误.
-- { host = "", port = , auth = function(so) , response = function(so) session, data }

local socket_channel = {} -- 模块表
-- 用于创建 channel 对象
local channel = {}
local channel_socket = {}
local channel_meta = { __index = channel }

-- 用于创建一个关联 socket id 的对象, channel.__sock 对象的原表
local channel_socket_meta = {
	__index = channel_socket,
	__gc = function(cs)
		local fd = cs[1]
		cs[1] = false
		if fd then
			socket.shutdown(fd)
		end
	end
}

-- 用于 socket 产生的错误
local socket_error = setmetatable({}, {__tostring = function() return "[Error: socket]" end })	-- alias for error object
socket_channel.error = socket_error

-- 创建 channel 对象, desc 是 table 类型, 具体介绍查看: https://github.com/cloudwu/skynet/wiki/SocketChannel
-- 对于协议模式一: desc 需要包含 host 和 port 字段;
-- 对于协议模式二: desc 需要包含 host, port, response 字段, response 对应的是函数类型, 作用是解析回应包;
function socket_channel.channel(desc)
	local c = {
		__host = assert(desc.host), -- ip 地址或者域名
		__port = assert(desc.port), -- port 端口号
		__backup = desc.backup, -- 备用的连接主机, table 类型: 存储的值如果是 table 类型, 那么格式是 { host = "", port = 1234 }; 如果值是字符串类型, 那么表示的 host, 连接时使用 __port.

		-- 由于连接可能发生在任何 request 之前（只要前一次操作检测到连接是断开状态就会重新发起连接），
		-- 所以 socket channel 支持认证流程，允许在建立连接后，立刻做一些交互。如果开启这个功能，
		-- 需要在创建 channel 时，填写一个 auth 函数。和 response 函数一样，会给它传入一个 sock 对象。
		-- auth 函数不需要返回值，如果认证失败，在 auth 函数中抛出 error 即可。
		__auth = desc.auth, -- 认证函数
		__response = desc.response,	-- It's for session mode, 用于 session 模式(协议模式二)
		__request = {},	-- request seq { response func or session }	-- It's for order mode, 请求序列 { 响应函数或者 session }, 用于顺序模式(协议模式一)
		__thread = {}, -- coroutine seq or session->coroutine map, 协程序列或者 session->coroutine 的 map 表
		__result = {}, -- response result { coroutine -> result }, 响应结果(true/false, 如果是 table 类型时, 说明该 socket 已经 close 了), { coroutine -> result } 表
		__result_data = {}, -- 响应的数据
		__connecting = {}, -- 记录其他调用 connect 的协程
		__sock = false, -- 以 channel_socket_meta 作为原表的对象, [1] 元素是 socket id
		__closed = false, -- 判断 channel 是否 close, 调用 close 方法
		__authcoroutine = false, -- 认证协程
		__nodelay = desc.nodelay, -- 设置 socket 的 nodelay 选项
		__overload_notify = desc.overload,
		__overload = false,
		__socket_meta = channel_socket_meta,
	}
	if desc.socket_read or desc.socket_readline then
		c.__socket_meta = {
			__index = {
				read = desc.socket_read or channel_socket.read,
				readline = desc.socket_readline or channel_socket.readline,
			},
			__gc = channel_socket_meta.__gc
		}
	end

	return setmetatable(c, channel_meta)
end

-- 关闭 socket, 传入参数 channel 对象
local function close_channel_socket(self)
	if self.__sock then
		local so = self.__sock
		self.__sock = false
		-- never raise error
		-- 绝不会抛出错误
		pcall(socket.close,so[1])
	end
end

-- 唤醒所有的协程, 并且清空所有记录的协程对象, 同时赋予错误信息.
local function wakeup_all(self, errmsg)
	if self.__response then -- 协议模式二

		-- __thread 是 session->coroutine 表, 清除 __thread
		for k,co in pairs(self.__thread) do
			self.__thread[k] = nil
			self.__result[co] = socket_error
			self.__result_data[co] = errmsg
			skynet.wakeup(co)
		end
	else -- 协议模式一
		-- 清除请求
		for i = 1, #self.__request do
			self.__request[i] = nil
		end

		-- __thread 是序列, 清除 __thread
		for i = 1, #self.__thread do
			local co = self.__thread[i]
			self.__thread[i] = nil
			if co then	-- ignore the close signal
				self.__result[co] = socket_error
				self.__result_data[co] = errmsg
				skynet.wakeup(co)
			end
		end
	end
end

-- 基于协议模式二, 通过 session 调度协程
local function dispatch_by_session(self)
	local response = self.__response
	-- response() return session
	-- response() 函数返回 session
	while self.__sock do
		local ok , session, result_ok, result_data, padding = pcall(response, self.__sock) -- 注意, 这里有可能会阻塞
		--[[
      返回值介绍:
      ok: 函数调用是否发生 error;
      session: 回应包的 session;
      result_ok: 包是否解析正确（同模式 1 ）;
      result_data: 回应内容;
      padding: 表明了后续是否还有该长消息的后续部分。
		--]]
		if ok and session then
			local co = self.__thread[session]
			if co then
				if padding and result_ok then
					-- If padding is true, append result_data to a table (self.__result_data[co])
					-- 如果 padding 为 true, 追加 result_data 到 self.__result_data[co]
					local result = self.__result_data[co] or {}
					self.__result_data[co] = result
					table.insert(result, result_data)
				else
					self.__thread[session] = nil
					self.__result[co] = result_ok
					if result_ok and self.__result_data[co] then
						table.insert(self.__result_data[co], result_data)
					else
						self.__result_data[co] = result_data
					end
					skynet.wakeup(co)
				end
			else
				self.__thread[session] = nil
				skynet.error("socket: unknown session :", session)
			end
		else
			close_channel_socket(self)
			local errormsg
			if session ~= socket_error then
				errormsg = session
			end
			wakeup_all(self, errormsg)
		end
	end
end

-- 基于协议模式一, 弹出 result 和协程
local function pop_response(self)
	while true do
		local func,co = table.remove(self.__request, 1), table.remove(self.__thread, 1)
		if func then
			return func, co
		end
		self.__wait_response = coroutine.running()
		skynet.wait(self.__wait_response)
	end
end

-- on close callback
local function autoclose_cb(self, fd)
	local sock = self.__sock
	if self.__wait_response and sock and sock[1] == fd then
		-- closed by peer
		skynet.error("socket closed by peer : ", self.__host, self.__port)
		close_channel_socket(self)
	end
end

-- 压入响应.
-- 协议模式一: response 是 1 个函数;
-- 协议模式二: response 是 session.
local function push_response(self, response, co)
	if self.__response then
		-- response is session
		-- response 参数是 session
		self.__thread[response] = co
	else
		-- response is a function, push it to __request
		-- response 是 1 个函数, 将它压入到 __request 中
		table.insert(self.__request, response)
		table.insert(self.__thread, co)
		if self.__wait_response then
			skynet.wakeup(self.__wait_response)
			self.__wait_response = nil
		end
	end
end

local function get_response(func, sock)
	local result_ok, result_data, padding = func(sock)
	if result_ok and padding then
		local result = { result_data }
		local index = 2
		repeat
			result_ok, result_data, padding = func(sock)
			if not result_ok then
				return result_ok, result_data
			end
			result[index] = result_data
			index = index + 1
		until not padding
		return true, result
	else
		return result_ok, result_data
	end
end

-- 基于模式一: 通过顺序来调度协程
local function dispatch_by_order(self)
	while self.__sock do
		local func, co = pop_response(self)
		if not co then
			-- close signal
			wakeup_all(self, "channel_closed")
			break
		end
		local sock = self.__sock
		if not sock then
			-- closed by peer
			self.__result[co] = socket_error
			skynet.wakeup(co)
			wakeup_all(self)
			break
		end
		local ok, result_ok, result_data = pcall(get_response, func, sock) -- 注意, 这里有可能会阻塞
		--[[
			返回值介绍:
			ok: 函数调用是否发生 error;
			result_ok: 包是否解析正确;
			result_data: 回应内容;
		--]]
		if ok then
			self.__result[co] = result_ok
			if result_ok and self.__result_data[co] then
				table.insert(self.__result_data[co], result_data)
			else
				self.__result_data[co] = result_data
			end
			skynet.wakeup(co)
		else
			close_channel_socket(self)
			local errmsg
			if result_ok ~= socket_error then
				errmsg = result_ok
			end
			self.__result[co] = socket_error
			self.__result_data[co] = errmsg
			skynet.wakeup(co)
			wakeup_all(self, errmsg)
		end
	end
end

local function dispatch_function(self)
	if self.__response then
		return dispatch_by_session
	else
		socket.onclose(self.__sock[1], function(fd)
			autoclose_cb(self, fd)
		end)
		return dispatch_by_order
	end
end

local function term_dispatch_thread(self)
	if not self.__response and self.__dispatch_thread then
		-- dispatch by order, send close signal to dispatch thread
		push_response(self, true, false)	-- (true, false) is close signal
	end
end

-- 连接主机, 成功连接返回 true
local function connect_once(self)
	if self.__closed then
		return false
	end

	local addr_list = {}
	local addr_set = {}

	local function _add_backup()
		if self.__backup then
			for _, addr in ipairs(self.__backup) do
				local host, port
				if type(addr) == "table" then
					host,port = addr.host, addr.port
				else
					host = addr
					port = self.__port
				end

				-- don't add the same host
				local hostkey = host..":"..port
				if not addr_set[hostkey] then
					addr_set[hostkey] = true
					table.insert(addr_list, { host = host, port = port })
				end
			end
		end
	end

	local function _next_addr()
		local addr =  table.remove(addr_list,1)
		if addr then
			skynet.error("socket: connect to backup host", addr.host, addr.port)
		end
		return addr
	end

	local function _connect_once(self, addr)
		local fd,err = socket.open(addr.host, addr.port)
		if not fd then
			-- try next one
			addr = _next_addr()
			if addr == nil then
				return false, err
			end
			return _connect_once(self, addr)
		end

		self.__host = addr.host
		self.__port = addr.port

		assert(not self.__sock and not self.__authcoroutine)
		-- term current dispatch thread (send a signal)
		term_dispatch_thread(self)

		if self.__nodelay then
			socketdriver.nodelay(fd)
		end

		-- register overload warning

		local overload = self.__overload_notify
		if overload then
			local function overload_trigger(id, size)
				if id == self.__sock[1] then
					if size == 0 then
						if self.__overload then
							self.__overload = false
							overload(false)
						end
					else
						if not self.__overload then
							self.__overload = true
							overload(true)
						else
							skynet.error(string.format("WARNING: %d K bytes need to send out (fd = %d %s:%s)", size, id, self.__host, self.__port))
						end
					end
				end
			end

			skynet.fork(overload_trigger, fd, 0)
			socket.warning(fd, overload_trigger)
		end

		while self.__dispatch_thread do
			-- wait for dispatch thread exit
			skynet.yield()
		end

		self.__sock = setmetatable( {fd} , self.__socket_meta )
		self.__dispatch_thread = skynet.fork(function()
			if self.__sock then
				-- self.__sock can be false (socket closed) if error during connecting, See #1513
				pcall(dispatch_function(self), self)
			end
			-- clear dispatch_thread
			self.__dispatch_thread = nil
		end)

		if self.__auth then
			self.__authcoroutine = coroutine.running()
			local ok , message = pcall(self.__auth, self)
			if not ok then
				close_channel_socket(self)
				if message ~= socket_error then
					self.__authcoroutine = false
					skynet.error("socket: auth failed", message)
				end
			end
			self.__authcoroutine = false
			if ok then
				if not self.__sock then
					-- auth may change host, so connect again
					return connect_once(self)
				end
				-- auth succ, go through
			else
				-- auth failed, try next addr
				_add_backup()	-- auth may add new backup hosts
				addr = _next_addr()
				if addr == nil then
					return false, "no more backup host"
				end
				return _connect_once(self, addr)
			end
		end

		return true
	end

	_add_backup()
	return _connect_once(self, { host = self.__host, port = self.__port })
end

-- 尝试连接到主机, 成功连接返回 true. once: true/false, 表示是否只连接 1 次, 如果为 false, 那么会无限的尝试连接.
local function try_connect(self , once)
	local t = 0
	while not self.__closed do
		local ok, err = connect_once(self)
		if ok then
			if not once then
				skynet.error("socket: connect to", self.__host, self.__port)
			end
			return
		elseif once then
			return err
		else
			skynet.error("socket: connect", err)
		end
		if t > 1000 then
			skynet.error("socket: try to reconnect", self.__host, self.__port)
			skynet.sleep(t)
			t = 0
		else
			skynet.sleep(t)
		end
		t = t + 100
	end
end

-- 检查连接是否有效, true 表示已经连接; false 表示已经 close; nil 表示还未连接.
local function check_connection(self)
	if self.__sock then
		local authco = self.__authcoroutine
		if socket.disconnected(self.__sock[1]) then
			-- closed by peer
			skynet.error("socket: disconnect detected ", self.__host, self.__port)
			close_channel_socket(self)
			if authco and authco == coroutine.running() then
				-- disconnected during auth, See #1513
				return false
			end
			return
		end
		if not authco then
			return true
		end
		if authco == coroutine.running() then
			-- authing
			return true
		end
	end
	if self.__closed then
		return false
	end
end

local function block_connect(self, once)
	local r = check_connection(self)
	if r ~= nil then
		return r
	end
	local err

	if #self.__connecting > 0 then
		-- connecting in other coroutine
		local co = coroutine.running()
		table.insert(self.__connecting, co)
		skynet.wait(co)
	else
		self.__connecting[1] = true
		err = try_connect(self, once)
		for i=2, #self.__connecting do
			local co = self.__connecting[i]
			self.__connecting[i] = nil
			skynet.wakeup(co)
		end
		self.__connecting[1] = nil
	end

	r = check_connection(self)
	if r == nil then
		skynet.error(string.format("Connect to %s:%d failed (%s)", self.__host, self.__port, err))
		error(socket_error)
	else
		return r
	end
end

-- 尝试连接一次。如果失败，抛出 error 。这里参数 true 表示只尝试一次，如果不填这个参数，则一直重试下去。
function channel:connect(once)
	self.__closed = false
	return block_connect(self, once)
end

-- 等待响应.
-- 协议模式一: response 是函数;
-- 协议模式二: response 是 session.
local function wait_for_response(self, response)
	local co = coroutine.running()
	push_response(self, response, co)
	skynet.wait(co)

	local result = self.__result[co]
	self.__result[co] = nil
	local result_data = self.__result_data[co]
	self.__result_data[co] = nil

	if result == socket_error then
		if result_data then
			error(result_data)
		else
			error(socket_error)
		end
	else
		assert(result, result_data)
		return result_data
	end
end

local socket_write = socket.write
local socket_lwrite = socket.lwrite

local function sock_err(self)
	close_channel_socket(self)
	wakeup_all(self)
	error(socket_error)
end

--[[
给连接的主机发送请求.
也可用于仅发包而不接收回应。只需要在 request 调用时不填写 response 即可。

在模式 1 下:
	request 是一个字符串，即请求包。
	response 是一个 function ，用来收取回应包。
	padding ，这是用来将体积巨大的消息拆分成多个包发出用的, padding 需求是一个 table ，里面有若干字符串。

在模式 2 下:
	第 2 个参数不再是 response 函数, 而是一个 session 。这个 session 可以是任意类型，
	但需要和 response 函数返回的类型一致。socket channel 会帮你匹配 session 而让 request 返回正确的值。
--]]
function channel:request(request, response, padding)
	assert(block_connect(self, true))	-- connect once
	local fd = self.__sock[1]

	if padding then
		-- padding may be a table, to support multi part request
		-- multi part request use low priority socket write
		-- now socket_lwrite returns as socket_write
		-- 多个请求使用低优先级的写队列.
		if not socket_lwrite(fd , request) then
			sock_err(self)
		end
		for _,v in ipairs(padding) do
			if not socket_lwrite(fd, v) then
				sock_err(self)
			end
		end
	else
		if not socket_write(fd , request) then
			sock_err(self)
		end
	end

	if response == nil then
		-- no response
		return
	end

	-- 等待回应, 并且返回
	return wait_for_response(self, response)
end

-- 用来单向接收一个包。
function channel:response(response)
	assert(block_connect(self))

	return wait_for_response(self, response)
end

-- 可以关闭一个 channel ，通常你可以不必主动关闭它，gc 会回收 channel 占用的资源。
function channel:close()
	if not self.__closed then
		term_dispatch_thread(self)
		self.__closed = true
		close_channel_socket(self)
	end
end

-- 设置 channel 的 host 和 port, 如果 channel 是连接状态, 那么将关闭之前的连接
function channel:changehost(host, port)
	self.__host = host
	if port then
		self.__port = port
	end
	if not self.__closed then
		close_channel_socket(self)
	end
end

-- 设置 __backup 选项
function channel:changebackup(backup)
	self.__backup = backup
end

channel_meta.__gc = channel.close

-- 给 socket 的函数做一次包装.
-- 作用: 首先方便使用语法糖 obj:func() 这种语法; 接着在内部实现中使用 obj[1] 元素; 最后对返回值做检查; 其实 obj 就是 channel.__sock
local function wrapper_socket_function(f)
	return function(self, ...)
		local result = f(self[1], ...)
		if not result then
			error(socket_error)
		else
			return result
		end
	end
end

-- 包装 socket.read
channel_socket.read = wrapper_socket_function(socket.read)

-- 包装 socket.readline
channel_socket.readline = wrapper_socket_function(socket.readline)

return socket_channel
