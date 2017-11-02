# spark-bigquery

This project provides a Google BigQuery data source (`com.miraisolutions.spark.bigquery.DefaultSource`) to Spark on top of [Spotify's spark-bigquery library](https://github.com/spotify/spark-bigquery).

This data source is used in the [sparkbq](https://github.com/miraisolutions/sparkbq) R package.

## Building

Due to dependency version mismatches between Apache Spark and Google client libraries (e.g. Google Guava) this project uses [`sbt-assembly`](https://github.com/sbt/sbt-assembly) to build a fat JAR using [shading](https://github.com/sbt/sbt-assembly#shading) to relocate relevant Google classes.

ProGuard may be used via the [`sbt-proguard`](https://github.com/sbt/sbt-proguard) plugin to further shrink the assembly size. Simply run `sbt proguard` to perform that step. The resulting JAR file can be found in the `target/proguard` folder.

## License

MIT License

Copyright (c) 2017 Mirai Solutions GmbH

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
