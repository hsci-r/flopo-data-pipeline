package fi.hsci

import org.apache.lucene.index.{DirectoryReader, DocValues, IndexWriter}
import org.apache.lucene.store.MMapDirectory
import org.joda.time.format.ISODateTimeFormat
import org.json4s.native.JsonParser.{FieldStart, Parser, parse, _}
import org.json4s.{JArray, JString, _}

import java.io.{File, FileInputStream, InputStreamReader}
import java.nio.file.FileSystems
import scala.collection.mutable
import scala.compat.java8.StreamConverters._

object YLEMetadataIndexer extends OctavoIndexer {

  class Reuse {
    val d = new FluidDocument()
    val url = new StringSDVFieldPair("url").r(d)
    val section = new StringSDVFieldPair("section").r(d)
    val storyLogo = new StringSDVFieldPair("story_logo").o(d)
    val creationTime = new LongPointSDVDateTimeFieldPair("time_created",ISODateTimeFormat.dateTimeNoMillis).r(d)
    val lastModified = new LongPointSDVDateTimeFieldPair("time_modified",ISODateTimeFormat.dateTimeNoMillis).r(d)
    val coverageFields = new StringSDVFieldPair("coverage").o(d)
    val sourcesFields = new TextSDVFieldPair("sources").o(d)
    //val subjectsFields = new StringSSDVFieldPair("subject").o(d)
    def clean() {
      d.clearOptional()
    }
  }

  case class ArticleInfo(id: String, url: String, startDate: String, modifiedDate: String, section: String,coverage: Option[String],sources: Option[String], subjects: Seq[String]) {
    def populate(r: Reuse): Unit = {
      r.clean()
      r.url.setValue(url)
      r.creationTime.setValue(startDate)
      r.lastModified.setValue(modifiedDate)
      r.section.setValue(section)
      coverage.foreach(r.coverageFields.setValue)
      sources.foreach(r.sourcesFields.setValue)
      subjects.foreach(new StringSSDVFieldPair("subject").o(r.d).setValue(_))
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
        if (!articleInfos.contains(documentId)) throw new IllegalArgumentException("Unknown documentId")
        articleInfos(documentId).populate(r)
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
    hasDocumentParts = opts.hasDocumentParts()
    val parts = if (hasDocumentParts) 4 else 3
    siw = iw(opts.index() + "/sentence_metadata_index", null, opts.indexMemoryMb() / parts, !opts.noMmap())
    dpiw = if (hasDocumentParts) iw(opts.index() + "/document_part_metadata_index", null, opts.indexMemoryMb() / parts, !opts.noMmap()) else null
    piw = iw(opts.index() + "/paragraph_metadata_index", null, opts.indexMemoryMb() / parts, !opts.noMmap())
    aiw = iw(opts.index() + "/document_metadata_index", null, opts.indexMemoryMb() / parts, !opts.noMmap())
    opts.directories().toIndexedSeq.flatMap(d => getFileTree(new File(d))).parStream.filter(_.getName.endsWith(".json")).forEach(file => {
      parse(new InputStreamReader(new FileInputStream(file)), (p: Parser) => {
        var token = p.nextToken
        while (token != FieldStart("data")) token = p.nextToken
        token = p.nextToken // OpenArr
        token = p.nextToken // OpenObj/CloseArr
        while (token != CloseArr) {
          assert(token == OpenObj, token)
          val obj = ObjParser.parseObject(p, Some(token))
          val id = (obj \ "id").asInstanceOf[JString].values
          val url = (obj \ "url" \ "full").asInstanceOf[JString].values
          val startDate = (obj \ "datePublished").asInstanceOf[JString].values
          val modifiedDate = (obj \ "dateContentModified").asInstanceOf[JString].values
          val section = (obj \ "publisher" \ "name").asInstanceOf[JString].values
          val coverage = (obj \ "coverage") match {
            case string2: JString => Some(string2.values)
            case _ => None
          }
          val sources = (obj \ "sources") match {
            case string2: JString => Some(string2.values)
            case _ => None
          }
          val subjects = (obj \ "subjects") match {
            case a: JArray => a.arr.filter(
              _ \ "exactMatch" match {
                case em: JArray => em.arr.exists(_.asInstanceOf[JString].values.startsWith("escenic:"))
                case _ => false
              }
            ).map(t => (t \ "title" \ "fi").asInstanceOf[JString].values)
            case _ => Seq.empty
          }
          articleInfos.synchronized(articleInfos.put(id,ArticleInfo(id,url,startDate,modifiedDate,section,coverage,sources,subjects)))
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
