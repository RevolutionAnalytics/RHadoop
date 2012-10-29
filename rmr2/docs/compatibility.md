# Compatibility testing for rmr 2.0.1
Please contribute with additional reports. To claim compatibility you need to run `R CMD  check path-to-rmr` successfully.
If you build your own Hadoop, see [Which Hadoop for rmr](https://github.com/RevolutionAnalytics/RHadoop/wiki/Which-Hadoop-for-rmr).
If you are interested in the compatibility chart for other releases, choose one from the drop down menu. on the top left. Not every release gets a complete round of testing, so typically a bug fix release (change in the third number only) is more compatible than the previous release, even if we don't have the resource to test it directly. 

<table>
<thead>
<tr><th>Hadoop</th><th>R</th><th>OS</th><th>Notes</th><th>Reporter</th></tr>
</thead>
<tbody>
<tr><td>CDH4.1.1</td><td>Revolution R 6.0</td><td>CentOS 5.6</td><td>64-bit, mr1 only</td><td>Revolution</td></tr>
<tr><td>CDH3u5</td><td>Revolution R 6.0</td><td>CentOS 5.6</td><td>64-bit</td><td>Revolution</td></tr>
<tr><td>Apache Hadoop 1.0.4</td><td>Revolution R 6.0</td><td>CentOS 5.6</td><td>64-bit</td><td>Revolution</td></tr>
<tr><td>MapR 2.0.1</td><td>Revolution R 6.0</td><td>CentOS 6.2</td><td>64-bit</td><td>MapR</td></tr></tbody>
</table>
