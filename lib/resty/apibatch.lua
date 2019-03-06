local cjson = require "cjson.safe"
local cjson_decode = cjson.decode
local cjson_encode = cjson.encode

-- TODO: add max requests per batch to config
local _M = { conf = {
    response_with_headers = true,
    methods_map = {
        PUT = ngx.HTTP_PUT,
        POST = ngx.HTTP_POST,
        OPTIONS = ngx.HTTP_OPTIONS,
        GET = ngx.HTTP_GET,
        DELETE = ngx.HTTP_DELETE,
        HTTP_HEAD = ngx.HTTP_HEAD,
    },
    allowed_methods = { PUT = true, GET = true, POST = true, OPTIONS = true, DELETE = true },
    allowed_paths = {}
} }

function set (list)
    local set = {}
    for _, l in ipairs(list) do
        set[l] = true
    end
    return set
end

function _M.error_response(message, status)
    local jsonStr = '{"data":[],"error":{"code":' .. status .. ',"message":"' .. message .. '"}}'
    ngx.header['Content-Type'] = 'application/json'
    ngx.status = status
    ngx.say(jsonStr)
    ngx.exit(status)
end

-- TODO: create init function for init_worker_by_lua_block
-- TODO: move configuration to lua_shared_dict
function _M.load_config(conf)
    if conf.allowed_methods and type(conf.allowed_methods) == 'table' then
        _M.conf.allowed_methods = set(conf.allowed_methods)
    end
    if conf.response_with_headers ~= nil then
        _M.conf.response_with_headers = conf.response_with_headers
    end
    -- TODO: allowed_paths
end

function _M.validate_request(conf)
    if ngx.req.get_method() ~= 'POST' then
        _M.error_response('Method is not allowed', 405)
    end
    if ngx.req.get_headers()['Content-Type'] ~= "application/json" then
        _M.error_response('Unsupported Content-Type', 415)
    end
    _M.load_config(conf)

    ngx.req.read_body()
    local body = ngx.req.get_body_data()
    if body == nil or not body then
        _M.error_response('Bad request - empty body', 400)
    end
    local req_rows = cjson_decode(body)
    if type(req_rows) ~= 'table' then
        _M.error_response('Bad request - body is not array', 400)
    end
    if next(req_rows) == nil then
        _M.error_response('Bad request - empty batch', 400)
    end

    return req_rows
end

function _M.validate_request_row(row, i)
    if type(row) ~= 'table'
            or (not row.relative_url)
            or (not row.method)
            or (not _M.conf.methods_map[row.method])
            or (_M.conf.allowed_methods[row.method] ~= true)
    then
        _M.error_response('Bad request item(' .. i .. ')', 400)
    end
end

function _M.parse_requests_from_req(conf)
    local req_rows = _M.validate_request(conf)
    local requests = {}
    for i, row in pairs(req_rows) do
        _M.validate_request_row(row, i)
        local opts = { method = _M.conf.methods_map[row.method] }
        if row.body then
            if type(row.body) == 'table' then
                row.body = cjson_encode(row.body)
            end
            opts.body = row.body
        end
        if row.args and type(row.args) == 'table' then
            opts.args = row.args
        end
        table.insert(requests, { row.relative_url, opts })
    end

    return requests
end

function _M.handle(conf)
    local requests = _M.parse_requests_from_req(conf)
    local responses = { ngx.location.capture_multi(requests) }
    local result = {}

    for i, resp in pairs(responses) do
        res = { code = resp.status, body = resp.body }
        if _M.conf.response_with_headers then
            res.headers = resp.header
        end
        table.insert(result, res)
    end

    ngx.header['Content-Type'] = 'application/json'
    ngx.status = 200
    ngx.say(cjson_encode(result))
    ngx.exit(200)
    return
end

return _M

