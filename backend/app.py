from flask import Flask, jsonify
import redis
import os
import sys

# 初始化Flask应用
app = Flask(__name__)

# 从环境变量获取Redis配置（对应K8s的ConfigMap和Secret）
# ConfigMap注入：REDIS_HOST、REDIS_PORT
# Secret注入：REDIS_PASSWORD
redis_host = os.getenv("REDIS_HOST", "localhost")
redis_port = int(os.getenv("REDIS_PORT", 6379))
redis_password = os.getenv("REDIS_PASSWORD", "")

# 初始化Redis连接
try:
    r = redis.Redis(
        host=redis_host,
        port=redis_port,
        password=redis_password,
        decode_responses=True,
        socket_timeout=5
    )
    # 测试连接
    r.ping()
    print(f"✅ Redis连接成功: {redis_host}:{redis_port}")
except Exception as e:
    print(f"❌ Redis连接失败: {str(e)}")
    r = None

# ------------------- 核心验收接口（任务3必须）-------------------
@app.route("/api/ping", methods=["GET"])
def ping():
    """
    验收必备接口：返回{"status":"ok"}
    任务3要求通过ELB公网IP访问此接口验证服务正常
    """
    return jsonify({
        "status": "ok",
        "message": "Flask后端服务运行正常",
        "redis_status": "connected" if r else "disconnected"
    })

# ------------------- 辅助测试接口（任务4持久化验证用）-------------------
@app.route("/api/redis/set/<key>/<value>", methods=["GET"])
def set_redis(key, value):
    """写入Redis，用于验证数据持久化"""
    if not r:
        return jsonify({"error": "Redis未连接"}), 500
    try:
        r.set(key, value)
        return jsonify({"status": "ok", "key": key, "value": value})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/redis/get/<key>", methods=["GET"])
def get_redis(key):
    """读取Redis，用于验证Pod重建后数据不丢失"""
    if not r:
        return jsonify({"error": "Redis未连接"}), 500
    value = r.get(key)
    if value is None:
        return jsonify({"error": "Key不存在"}), 404
    return jsonify({"status": "ok", "key": key, "value": value})

# ------------------- 应用启动 -------------------
if __name__ == "__main__":
    # 监听0.0.0.0:5000，和Dockerfile暴露端口一致
    app.run(host="0.0.0.0", port=5000, debug=False)