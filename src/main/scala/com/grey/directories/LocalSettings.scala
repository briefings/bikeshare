package com.grey.directories

import java.nio.file.Paths

class LocalSettings {

  // Of environment
  val projectDirectory: String = System.getProperty("user.dir")
  val sep: String = System.getProperty("file.separator")

  // Base names
  val names = List("src", "main", "resources")

  // Resources directory
  val resourcesDirectory: String = Paths.get(projectDirectory, names: _* ).toString

}
