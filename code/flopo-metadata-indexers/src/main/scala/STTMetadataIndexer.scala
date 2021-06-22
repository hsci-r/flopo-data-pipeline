package fi.hsci

import org.apache.lucene.index.{DirectoryReader, DocValues, IndexWriter}
import org.apache.lucene.store.MMapDirectory
import org.joda.time.format.DateTimeFormat

import java.io.{File, FileInputStream}
import java.nio.file.FileSystems
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.compat.java8.StreamConverters._
import XMLEventReaderSupport.{EvComment, EvElemEnd, EvElemStart, EvEntityRef, EvEvent, EvText, getXMLEventReader}
import org.joda.time.{DateTimeZone, IllegalInstantException}

import scala.xml.parsing.XhtmlEntities

object STTMetadataIndexer extends OctavoIndexer {

  class Reuse {
    val d = new FluidDocument()
    val url = new StringSDVFieldPair("url").r(d)
    val section = new StringSDVFieldPair("section").r(d)
    val creationTime = new LongPointSDVFieldPair("time_created").r(d)
    val lastModified = new LongPointSDVFieldPair("time_modified").r(d)
    val version = new StringSDVFieldPair("version").r(d)
    val urgency = new IntPointNDVFieldPair("urgency").r(d)
    val genre = new StringSDVFieldPair("genre").r(d)
    val creditline = new TextSDVFieldPair("creditline").r(d)
    val byline = new TextSDVFieldPair("byline").r(d)
    def clean() {
      d.clearOptional()
    }
  }

  val dtf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(DateTimeZone.forID("Europe/Helsinki"))

  case class ArticleInfo(id: String, startDate: String, modifiedDate: String, version: String, urgency: Int, genre: String, creditline: String, byline: String, subjects: scala.collection.Seq[String]) {
    def populate(r: Reuse): Unit = {
      r.clean()
      val url = s"https://a3s.fi/flopo-stt-${id.last}/$id.html"
      r.url.setValue(url)
      val startDateMillis = try {
        dtf.parseMillis(startDate)
      } catch {
        case _: IllegalInstantException => dtf.parseMillis(startDate.replace("T03:","T04:"))
      }
      r.creationTime.setValue(startDateMillis,startDate)
      val modifiedDateMillis = try {
        dtf.parseMillis(modifiedDate)
      } catch {
        case _: IllegalInstantException => dtf.parseMillis(modifiedDate.replace("T03:","T04:"))
      }
      r.lastModified.setValue(modifiedDateMillis,modifiedDate)
      r.version.setValue(version)
      r.urgency.setValue(urgency)
      r.genre.setValue(genre)
      r.creditline.setValue(creditline)
      r.byline.setValue(byline)
      subjects.foreach(new StringSSDVFieldPair("subject").o(r.d).setValue(_))
    }
  }

  private def decodeEntity(entity: String): String = {
    XhtmlEntities.entMap.get(entity) match {
      case Some(chr) => chr.toString
      case None =>
        logger.warn("Encountered unknown entity "+entity)
        '〈' + entity + '〉'
    }
  }
  private def readContents(element: String)(implicit xml: Iterator[EvEvent]): String = {
    var break = false
    val content = new StringBuilder()
    while (xml.hasNext && !break) xml.next match {
      case EvElemStart(_,_,_) =>
      case EvText(text,_)  => content.append(text)
      case er: EvEntityRef => content.append(decodeEntity(er.entity))
      case EvComment(comment) if comment == " unknown entity apos; " => content.append('\'')
      case EvComment(comment) if comment.startsWith(" unknown entity") =>
        val entity = comment.substring(16, comment.length - 2)
        content.append(decodeEntity(entity))
      case EvComment(comment) =>
        logger.debug("Encountered comment: "+comment)
      case EvElemEnd(_,element) => break = true
    }
    content.toString.trim
  }

  object ArticleInfo {
    def apply(file: File): ArticleInfo = {
      val fis = new FileInputStream(file)
      implicit val xml = getXMLEventReader(fis, "UTF-8")
      val id = file.getName.substring(0,file.getName.indexOf('.'))
      var version = ""
      var timePublished = ""
      var timeModified = ""
      var urgency=""
      var creditline = ""
      var byline = ""
      var department = ""
      var genre = ""
      val subjects = new ArrayBuffer[String]
      while (xml.hasNext) xml.next() match {
        case EvElemStart(_,"contentCreated",_) =>
          val c = readContents("contentCreated")
          if (c.nonEmpty) timePublished = c
        case EvElemStart(_,"firstCreated",_) =>
          val c = readContents("firstCreated")
          if (c.nonEmpty) timePublished = c
        case EvElemStart(_,"contentModified",_) =>
          val c = readContents("contentModified")
          if (c.nonEmpty) timeModified = c
        case EvElemStart(_,"versionCreated",_) =>
          val c = readContents("versionCreated")
          if (c.nonEmpty) timeModified = c
        case EvElemStart(_,"urgency",_) => urgency=readContents("urgency")
        case EvElemStart(_,"located",_) =>
        case EvElemStart(_,"creditline",_) => creditline=readContents("creditline")
        case EvElemStart(_,"by",_) => byline = readContents("by")
        case EvElemStart(_,"subject",attrs) =>
          val qcode = attrs("qcode")
          if (qcode.startsWith("sttdepartment:")) department = readContents("subject")
          else subjects += readContents("subject")
        case EvElemStart(_,"genre",attrs) =>
          val qcode = attrs("qcode")
          if (qcode.startsWith("sttversion:"))
            version = readContents("genre")
          else genre = readContents("genre")
        case _ =>
      }
      fis.close()
      ArticleInfo(id,timePublished,timeModified,version,urgency.toInt,genre,if (creditline=="") "uncredited" else creditline,if (byline == "") "anonymous" else byline,subjects)
    }
  }

  def process(path: String, iw: IndexWriter): Unit = {
    val r = new Reuse()
    val ir = DirectoryReader.open(new MMapDirectory(FileSystems.getDefault.getPath(path))).leaves().get(0).reader
    logger.info("Going to process "+ir.maxDoc+" documents in "+path+".")
    val dv = DocValues.getSorted(ir, "document_id")
    var lastDocumentId: String = null
    for (d <- 0 until ir.maxDoc) {
      dv.advance(d)
      val documentId = dv.binaryValue.utf8ToString
      if (documentId!=lastDocumentId) {
        lastDocumentId = documentId
        r.clean()
        if (!articleInfo.contains(documentId))
          logger.error(s"Unknown documentId $documentId. It's metadata will be wrong! Continuing only so that you can see all such errors")
        else articleInfo(documentId).populate(r)
      }
      iw.addDocument(r.d)
    }
  }

  var hasDocumentParts = false
  var siw, piw, dpiw, aiw = null.asInstanceOf[IndexWriter]

  val articleInfo = new mutable.HashMap[String,ArticleInfo]

  def main(args: Array[String]): Unit = {
    val opts = new AOctavoOpts(args) {
      val hasDocumentParts = opt[Boolean](default = Some(false))
      verify()
    }
    numWorkers = opts.workers()
    hasDocumentParts = opts.hasDocumentParts()
    val parts = if (hasDocumentParts) 4 else 3
    siw = iw(opts.index() + "/sentence_metadata_index", null, opts.indexMemoryMb() / parts, !opts.noMmap())
    dpiw = if (hasDocumentParts) iw(opts.index() + "/document_part_metadata_index", null, opts.indexMemoryMb() / parts, !opts.noMmap()) else null
    piw = iw(opts.index() + "/paragraph_metadata_index", null, opts.indexMemoryMb() / parts, !opts.noMmap())
    aiw = iw(opts.index() + "/document_metadata_index", null, opts.indexMemoryMb() / parts, !opts.noMmap())
    var i = 0
    opts.directories().to(LazyList).flatMap(d => getFileTree(new File(d))).seqStream.parallel.filter(_.getName.endsWith(".xml")).forEach(file => {
      val ai = ArticleInfo(file)
      articleInfo.synchronized(articleInfo.put(ai.id,ai))
      if ((i & 32767) == 0) logger.info(s"Processed ${ai.id}.")
      i+=1
    })
    var tasks = Seq(
      runInOtherThread(
        () => {
          process(opts.index() + "/sentence_index", siw)
          close(siw)
          merge(opts.index() + "/sentence_metadata_index", null, opts.indexMemoryMb() / parts, null)
        }
      ),
      runInOtherThread(
        () => {
          process(opts.index()+"/paragraph_index",piw)
          close(piw)
          merge(opts.index()+"/paragraph_metadata_index", null, opts.indexMemoryMb()/parts, null)
        }
      ),
      runInOtherThread(
        () => {
          process(opts.index()+"/document_index",aiw)
          close(aiw)
          merge(opts.index()+"/document_metadata_index", null, opts.indexMemoryMb()/parts, null)
        }
      )
    )
    if (opts.hasDocumentParts()) tasks = tasks :+ runInOtherThread(
      () => {
        process(opts.index()+"/document_part_index", dpiw)
        close(dpiw)
        merge(opts.index()+"/document_part_metadata_index", null, opts.indexMemoryMb()/parts, null)
      }
    )
    waitForTasks(tasks:_*)
  }
}
