package almeida.rochapaulo.spark.streaming.utils

import org.slf4j.LoggerFactory

/**
  * Created by rochapaulo on 28/04/16.
  */
trait Logging {

  def logger = _logger
  private lazy val _logger = LoggerFactory.getLogger(getClass.getSimpleName)

}
