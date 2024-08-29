# 第一阶段：构建阶段
FROM rust:latest as builder

# 设置工作目录
WORKDIR /usr/src/myapp

# 将 Cargo.toml 和 Cargo.lock 复制到容器中
COPY Cargo.toml Cargo.lock ./

COPY src ./src

# 编译最终的可执行文件
RUN cargo build --release

# 第二阶段：运行阶段
FROM debian:buster-slim

# 设置工作目录
WORKDIR /usr/local/bin

# 从构建阶段复制编译后的可执行文件
COPY --from=builder /usr/src/myapp/target/release/server .
RUN apt update
RUN apt install -y libssl-dev

# 暴露服务端口（如果适用）
EXPOSE 13000

# 设置启动命令
CMD ["./server"]