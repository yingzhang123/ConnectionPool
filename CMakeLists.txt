cmake_minimum_required(VERSION 2.5)
project(connectionpool)

# # mymuduo最终编译成so动态库，设置动态库的路径，放在根目录的lib文件夹下面
# set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)

# 设置调试信息 以及 启动C++11语言标准
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -std=c++11 -fPIC")

# 定义参与编译的源代码文件 
aux_source_directory(. SRC_LIST)

# # 编译生成动态库mymuduo
# add_library(mymuduo SHARED ${SRC_LIST})


add_executable(main ${SRC_LIST})
# 指定可执行文件链接时需要依赖的库文件
target_link_libraries(main mysqlclient pthread)