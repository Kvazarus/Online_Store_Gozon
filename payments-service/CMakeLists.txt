cmake_minimum_required(VERSION 3.14)
project(payments_service)

set(CMAKE_CXX_STANDARD 17)

find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(PQXX REQUIRED libpqxx)

find_package(Boost REQUIRED COMPONENTS system chrono)
find_library(RABBITMQ_LIB SimpleAmqpClient REQUIRED)
find_library(RABBITMQ_C_LIB rabbitmq REQUIRED)

add_executable(payments_service main.cpp)

target_link_libraries(payments_service PRIVATE
        Threads::Threads
        ${PQXX_LIBRARIES}
        ${RABBITMQ_LIB}
        ${RABBITMQ_C_LIB}
        Boost::system
        Boost::chrono
)

target_include_directories(payments_service PRIVATE
        ${PQXX_INCLUDE_DIRS}
        /usr/include
)