# -------------------------------------------------------------------------
# 📁 Dockerfile
# (V3 架构的 Python 基础镜像)
# -------------------------------------------------------------------------

# --- 阶段 1: "builder" ---
# 安装所有依赖
FROM python:3.11-slim AS builder

# 设置工作目录
WORKDIR /app

# (可选) 更新 apt 并安装构建工具 (如果某些库需要 C 扩展)
# RUN apt-get update && apt-get install -y build-essential

# 复制 requirements.txt
COPY requirements.txt .

# ❗️ 安装依赖到 Python 虚拟环境
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir -r requirements.txt

# --- 阶段 2: "production" ---
# 最终的轻量级镜像
FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# ❗️ 从 "builder" 阶段复制已安装的虚拟环境
COPY --from=builder /opt/venv /opt/venv

# ❗️ 复制我们所有的 Python 源代码 (shared, datastore, services)
COPY . .

# 设置环境变量，让 Python 使用该虚拟环境
ENV PATH="/opt/venv/bin:$PATH"

# (默认命令 - 通常会被 docker-compose.yml 覆盖)
# (这里我们用一个 sleep 来保持容器存活, 以便调试)
CMD ["sleep", "infinity"]