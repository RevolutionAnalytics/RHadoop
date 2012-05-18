# What's new

* A new vectorized API supports a vectorized style of programming, an efficient programming style for R which can achieve C-like 
speeds when followed consistently. The old API is still the default and has undergone only cosmetic changes, but when 
efficiency is important and with small record sizes using the vectorized interface is recommended. On the other hand the
old API allows slightly simpler programs and has better compatibility with complext R objects used as keys and values. See the 
[Introduction to the vectorized API](introduction-to-vectorized-API.md) for more information.
* In support of the vectorized API, there are completely new C implementations of serialization and deserialization from and to 
the typedbytes serialization format. This format is also important to exchange data with other members 
of the Hadoop system as typedbytes is the preferred serialiation format for non-java streaming applications (HADOOP-1722). 
The implementation supports the most common and useful cases for R users but is not yet fully adherent to the specification.
* All formats are compatible with the new vectorized API but "text" and "csv" readers and writers are substantially faster
and the "csv" reader is more stable.

