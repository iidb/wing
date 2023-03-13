# Wing

## Clone并创建自己的private repo

1. 在 [这里](https://github.com/new) 创建一个新的空repo并**确保权限为private**。注意，Github自带的fork无法将visibility修改为`private`。假设新建的repo为`git@github.com:your-id/your-repo.git`。

2. 在自己的机器或者我们提供的server上clone自己的repo：

```shell
git clone git@github.com:your-id/your-repo.git wing
cd wing
```

3. 从课程的public repo pull代码，并push到自己的repo中：

```shell
git remote add public git@github.com:iidb/wing.git
git pull public main
git push origin
```

未来如果实验框架有更新，则可以用`git pull public main`将更新pull下来。

## Build

### 本地编译

Wing使用C++20，你可能需要升级编译器。

Windows：[Mingw-w64](https://winlibs.com/)

Linux：更新GCC版本到10或以上。或者使用Clang。

使用G++编译命令：

```shell
mkdir build
cd build
cmake .. -DBUILD_JIT=OFF -DCMAKE_BUILD_TYPE="release" -DCMAKE_CXX_COMPILER="g++" -DCMAKE_C_COMPILER="gcc"
cmake --build . -j
```

使用Clang编译命令：

```shell
mkdir build
cd build
cmake .. -DBUILD_JIT=OFF -DCMAKE_BUILD_TYPE="release" -DCMAKE_CXX_COMPILER="clang++" -DCMAKE_C_COMPILER="clang"
cmake --build . -j
```

编译后会产生wing的命令行版本和测试程序。wing的命令行用法见docs/pre.md。

调试时，用debug版本编译，使用`-DCMAKE_BUILD_TYPE="debug"`。强烈建议release版本和debug版本存在不同文件夹，如`build-release`和`build-debug`，这样在切换模式时就不用全部重新编译。

使用LLVM JIT时，使用`-DBUILD_JIT=ON`。LLVM最好是11及以上版本，以下也可以试试。[Linux安装LLVM-dev](https://apt.llvm.org/)，Windows很麻烦。

在`build`目录下，Linux下可以直接用`make -j线程数`，或者自动将线程数设置为核数`make -j$(nproc)`，你可以在`~/.bashrc`里alias一下：`alias jmake='make -j$(nproc)'`。Windows可能是`mingw32-make -j`之类的。。。

P.S. `make -j`不限制使用的线程数，因此如果核数不够的话可能并不会变快，而且还会增大CPU资源的抢占并消耗更多内存。

使用其他构建系统（如ninja）: `-G "Ninja"`

如果你的电脑较慢，强烈建议编译release和debug两份，这样就不用每次改build type的时候重新全部编译。而且可以把make换成ninja。

### 通过Docker编译（不推荐作为开发环境，但与autolab评测环境一致，可以测试使用）

用docker构建镜像，镜像中包含了编译运行wing所需的最基本环境：

```shell
docker build . -t wing
docker create -it --name wing -v ./:/wing wing
```

通过以下命令进入Docker container中运行wing：

```shell
docker start -ai wing
```

wing的目录位于/wing下。

## Documentation

有关命令行用法、wing实现的SQL语法的文档见docs/。

## LICENSE

Except as otherwise noted (below and/or in individual files), this project is licensed under MIT license.
