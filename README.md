拟构建一个使用H5存储格式的文件存储工具，利用h5py库进行封装，并与Pandas，Numpy实现较好兼容

# 目前已实现的功能

## 文件树查看

h5文件内部储存结构，类似于win文件夹存储，h5.group对应文件夹，h5.dataset对应文件

> H5DataBase("FileName.h5").h5dir(attrs=False, dir_only=False)
>
> attrs:  默认不显示属性
>
> dir_only: 默认显示目录及文件

```python
from H5DataBase import H5DataBase
H5DataBase(h5FilePath='demo.h5').h5dir()

'''
<dir>  ~
<dir>  ~/meatData
  <file> <5.00 B>  ~/meatData/columns.int32
  <file> <10.00 B>  ~/meatData/index.uint64
<dir>  ~/pivotData
  <file> <50.00 B>  ~/pivotData/random_values.float64
'''
```

## 导出数据

导出h5中的指定dataset

```python
# 查看demo.h5文件中有哪些dataset
H5DataBase(h5FilePath='demo.h5').known_data
'''
['columns', 'index', 'random_values']
'''
# 导出指定的dataset
H5DataBase(h5FilePath='demo.h5').load_h5data('index')
```

## 将pandas.pivot格式数据便捷写入h5文件

使用write_pivotDF_to_h5data方法，可以将pandas.pivot格式数据写入h5文件

> 虽然pd.to_hdf也可以实现，但是文件结构不直观（不能便捷的增添数据），而且不能共用index, columns,  故有了此项目，希望对您有所帮助

```python
H5DataBase.write_pivotDF_to_h5data(h5FilePath='demo.h5',pivotDF=data_df,pivotKey='random_values',rewrite=True)
H5DataBase(h5FilePath='demo.h5').h5dir()  # 存储文件后查看结构
'''
<dir>  ~
<dir>  ~/meatData
  <file> <5.00 B>  ~/meatData/columns.int32
  <file> <10.00 B>  ~/meatData/index.uint64
<dir>  ~/pivotData
  <file> <50.00 B>  ~/pivotData/random_values.float64
'''
```

## 导出数据为pandas.pivot格式

demo.h5中，共有三个dataset，分别为index，columns及random_data，为了实现与pandas.pivot数据格式的便捷读取，有load_pivotDF_from_h5data方法，可以读取h5为pandas.pivot格式

> 需要注意的是h5文件中的数据组织方式必须按照文件树中展示的方式，即metaData/index, metaData/columns, pivotData/data

```python
H5DataBase(h5FilePath='demo.h5').load_pivotDF_from_h5data('random_values')
```

## 添加同index，columns的pandas.pivot格式数据

add_pivotDF_to_h5data方法，可以将相同index，和columns的pivot格式数据添加至现有的h5文件中，通过公用metaData，达到高效存储目的

```python
H5DataBase.add_pivotDF_to_h5data(h5FilePath='demo.h5',pivotDF=data_df,pivotKey='random_values_copy')
H5DataBase('demo').h5dir()  # 查看添加后的文件结构
'''
<dir>  ~
<dir>  ~/meatData
  <file> <5.00 B>  ~/meatData/columns.int32
  <file> <10.00 B>  ~/meatData/index.uint64
  view_dtype = <M8[ns]
<dir>  ~/pivotData
  <file> <50.00 B>  ~/pivotData/random_values.float64
  <file> <50.00 B>  ~/pivotData/random_values_copy.float64
'''
```

## TODO List

- Ts（时序数据，相当于pandas.melt）数据格式读写功能开发
- dataSet重复命名解决
- 多种数据格式兼容（目前已兼容index的datetime[ns], datatime[D]）
- 高效的分块读写开发