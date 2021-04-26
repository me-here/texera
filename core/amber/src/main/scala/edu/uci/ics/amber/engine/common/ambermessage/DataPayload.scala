package edu.uci.ics.amber.engine.common.ambermessage

import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{LinkIdentity, VirtualIdentity}

sealed trait DataPayload extends Serializable {
  val size: Int
}

final case class EndOfUpstream() extends DataPayload {
  override val size: Int = 1
}

final case class InputLinking(link: LinkIdentity) extends DataPayload {
  override val size: Int = 0
}

final case class DataFrame(frame: Array[ITuple]) extends DataPayload {
  override val size: Int = frame.length
  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[DataFrame]) return false
    val other = obj.asInstanceOf[DataFrame]
    if (other eq null) return false
    if (frame.length != other.frame.length) {
      return false
    }
    var i = 0
    while (i < frame.length) {
      if (frame(i) != other.frame(i)) {
        return false
      }
      i += 1
    }
    true
  }
}
