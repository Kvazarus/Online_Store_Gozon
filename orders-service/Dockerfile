FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential cmake git pkg-config \
    libboost-all-dev libpqxx-dev libtbb-dev \
    librabbitmq-dev \
    libasio-dev \
    && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/alanxz/SimpleAmqpClient.git \
    && cd SimpleAmqpClient \
    && mkdir build && cd build \
    && cmake .. -DCMAKE_INSTALL_PREFIX=/usr \
    && make -j4 && make install \
    && cd ../.. && rm -rf SimpleAmqpClient

RUN git clone https://github.com/CrowCpp/Crow.git \
    && cp -r Crow/include/* /usr/include/ \
    && rm -rf Crow

WORKDIR /app
COPY . .
RUN mkdir build && cd build && cmake .. && make

CMD ["./build/orders_service"]