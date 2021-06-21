package fi.hsci

import org.apache.lucene.index.{DirectoryReader, DocValues, IndexWriter}
import org.apache.lucene.store.MMapDirectory
import org.joda.time.format.ISODateTimeFormat
import org.json4s.native.JsonParser.{Parser, parse, _}
import org.json4s.{JString, _}

import java.io.{File, FileInputStream, InputStreamReader}
import java.nio.file.FileSystems
import scala.collection.mutable
import scala.compat.java8.StreamConverters._

object ILMetadataIndexer extends OctavoIndexer {

  class Reuse {
    val d = new FluidDocument()
    val url = new StringSDVFieldPair("url").r(d)
    val section = new StringSDVFieldPair("section").r(d)
    val creationTime = new LongPointSDVDateTimeFieldPair("time_created",ISODateTimeFormat.dateTimeNoMillis).r(d)
    //val authorFields = new TextSSDVFieldPair("author").o(d)
    def clean() {
      d.clearOptional()
    }
  }

  case class ArticleInfo(id: String, time_created: String, section: String,authors: Seq[String]) {
    val url = s"https://www.iltalehti.fi/${section}/${id}"
    def populate(r: Reuse): Unit = {
      r.clean()
      r.url.setValue(url)
      r.creationTime.setValue(time_created)
      r.section.setValue(section)
      authors.foreach(new TextSSDVFieldPair("author").o(r.d).setValue)
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
        if (!articleInfos.contains(documentId))
          logger.error(s"Unknown documentId $documentId. It's metadata will be wrong! Continuing only so that you can see all such errors")
        else articleInfos(documentId).populate(r)
      }
      iw.addDocument(r.d)
    }
  }

  var hasDocumentParts = false
  var siw, piw, dpiw, aiw = null.asInstanceOf[IndexWriter]

  val articleInfos = new mutable.HashMap[String,ArticleInfo]

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
    opts.directories().toIndexedSeq.flatMap(d => getFileTree(new File(d))).parStream.filter(_.getName.endsWith(".json")).forEach(file => {
      parse(new InputStreamReader(new FileInputStream(file)), (p: Parser) => {
        var token = p.nextToken // OpenArr
        token = p.nextToken // OpenObj/CloseArr
        while (token != CloseArr) {
          assert(token == OpenObj, token)
          val obj = ObjParser.parseObject(p, Some(token))
          val id = (obj \ "article_id").asInstanceOf[JString].values
          val section = (obj \ "category").asInstanceOf[JString].values
          val time_created = (obj \ "published_at").asInstanceOf[JString].values
          val authors = obj \ "authors" match {
            case JString(authors) if authors.nonEmpty => authors.split(",").map(_.strip)
            case _ => Array.empty[String]
          }
          articleInfos.synchronized(articleInfos.put(id,ArticleInfo(id,time_created,section,authors)))
          token = p.nextToken // OpenObj/CloseArr
        }
      })
      logger.info("File "+file+" processed.")
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
