import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{AsyncWordSpec, Matchers}

trait BaseSpec extends AsyncWordSpec with Matchers with LazyLogging
