name := "TianHongSparkCalc"

version := "1.0"

scalaVersion := "2.11.8"



unmanagedJars in Compile ++= {
  val base = baseDirectory.value
  val baseDirectories = (base / "dependency") +++ (base / "jars" / "*" )  //文件夹路径
  val customJars = (baseDirectories ** "*.jar")  //匹配文件类型
  customJars.classpath
}


