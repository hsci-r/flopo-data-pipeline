package fi.hsci

import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import org.apache.lucene.index.{DirectoryReader, DocValues, IndexWriter}
import org.apache.lucene.store.MMapDirectory
import org.joda.time.format.DateTimeFormat
import org.json4s.native.JsonMethods
import org.json4s.{JBool, JNothing, JString}

import java.io.File
import java.nio.file.FileSystems
import scala.collection.mutable

object HSMetadataIndexer extends OctavoIndexer {

  class Reuse {
    val d = new FluidDocument()
    val url = new StringSDVFieldPair("url").r(d)
    val section = new StringSDVFieldPair("section").r(d)
    val storyLogo = new StringSDVFieldPair("story_logo").o(d)
    val creationTime = new LongPointSDVDateTimeFieldPair("time_created",DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ssZZ")).r(d)
    val lastModified = new LongPointSDVDateTimeFieldPair("time_modified",DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ssZZ")).r(d)
    val byLine = new TextSDVFieldPair("byline").o(d)
    val paid = new StringSDVFieldPair("paid").r(d)
    //val edition = new StringSSDVFieldPair("edition").o(d)
    def clean() {
      d.clearOptional()
    }
  }

  object NodeType extends Enumeration {
    val Normal, Edition = Value
    def apply(s: String): NodeType.Value = s match {
      case "edition" => NodeType.Edition
      case "normal" => NodeType.Normal
      case o => throw new IllegalArgumentException("Unknown NodeType " + o)
    }
  }

/*  case class NodeInfo(parentId: Int, url: String, title: String, description: String, nodeType: NodeType.Value, startDate: String)
  object NodeInfo {
    def apply(row: Seq[String]): NodeInfo =
    // fields: 0:id,1:product,2:parent,3:url,4:title,5:description,6:style,7:custom,8:nodetype,9:startdate,10:enddate
      NodeInfo(if (row(2)==null) -1 else row(2).toInt,row(3),row(4),row(5),row(6),row(7),NodeType(row(8)),row(9))
  }*/

  case class ArticleInfo(id: String, startDate: String, modifiedDate: String, storyLogo: Option[String], byLine: String, paid: Boolean, section: String, sectionUrl: String, editions: Iterable[String]) {
    val url = "https://www.hs.fi"+sectionUrl+"/art-"+id+".html"
    def populate(r: Reuse): Unit = {
      r.clean()
      r.url.setValue(url)
      r.creationTime.setValue(startDate)
      r.lastModified.setValue(modifiedDate)
      storyLogo.foreach(r.storyLogo.setValue)
      r.byLine.setValue(byLine)
      if (paid) r.paid.setValue("true") else r.paid.setValue("false")
      r.section.setValue(section)
      editions.foreach(new StringSSDVFieldPair("edition").o(r.d).setValue(_))
    }
  }

  object ArticleInfo {
    val noEditions = Seq(("no edition","/unknown"))
    def apply(row: Seq[String]): ArticleInfo = {
      // fields: 0:id,1:resourcetype,2:startdate,3:modifieddate,4:title,5:data,6:custom,7:timestamp,8:nodeid,9:body,10:splitbody
      val dataJson = JsonMethods.parse(row(5))
      val byLine = dataJson \ "byLine" match {
        case JNothing => "anonymous"
        case JString(s) => s
      }
      val paid = dataJson \ "paid" match {
        case JNothing => false
        case JBool(b) => b
      }
      val customJson = JsonMethods.parse(row(6))
      val storyLogo = customJson \ "storyLogo" match {
        case JNothing => None
        case JString(s) => Some(s)
      }
      val ceditions = if (!assetsToEditions.contains(row.head)) noEditions else assetsToEditions(row.head).map(editions(_))
      val (section,sectionUrl) = if (row(8)==null) {
        val sectionUrl = if (ceditions.isEmpty) {
          logger.warn("No node associations for "+row)
          "/unknown"
        } else ceditions.head._2
        ("no section",sectionUrl)
      } else {
        val sid = row(8).toInt
        if (!sections.contains(sid)) {
          logger.warn("Primary node association "+sid+" not identified as a section for "+row)
          ("no section","/unknown")
        } else sections(sid)
      }
      ArticleInfo(row.head,row(2),row(3),storyLogo,byLine,paid,section,sectionUrl,ceditions.map(_._1))
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
        if (!articleInfo.contains(documentId)) throw new IllegalArgumentException("Unknown documentId")
        articleInfo(documentId).populate(r)
      }
      iw.addDocument(r.d)
    }
  }

  var hasDocumentParts = false
  var siw, piw, dpiw, aiw = null.asInstanceOf[IndexWriter]

  val sections = new mutable.HashMap[Int,(String,String)]
  val editions = new mutable.HashMap[Int,(String,String)]
  val assetsToEditions = new mutable.HashMap[String,mutable.HashSet[Int]]
  val articleInfo = new mutable.HashMap[String,ArticleInfo]

  def parseCSV(f: String, hasHeaders: Boolean = true): Iterator[Array[String]] = new Iterator[Array[String]] {

    private val s = new CsvParserSettings()
    s.setHeaderExtractionEnabled(hasHeaders)
    s.setMaxCharsPerColumn(-1)

    val parser = new CsvParser(s)
    parser.beginParsing(new File(f))

    var cur: Array[String] = _

    override def hasNext: Boolean = {
      if (cur == null) cur = parser.parseNext()
      cur != null
    }

    override def next(): Array[String] = {
      if (cur == null) return parser.parseNext()
      val ret = cur
      cur = null
      ret
    }

  }



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
    val dir = opts.directories().head
    val nr = parseCSV(dir+"/node_output.csv")
    logger.info("Reading "+dir+"/node_output.csv")
    // fields: 0:id,1:product,2:parent,3:url,4:title,5:description,6:style,7:custom,8:nodetype,9:startdate,10:enddate
    for (row <- nr) {
      if (row(8)=="edition") editions.put(row.head.toInt,(row(4),row(3)))
      else sections.put(row.head.toInt,(row(4),row(3)))
    }
    logger.info("Reading "+dir+"/aassetnoderelation_output.csv")
    val nar = parseCSV(dir+"/assetnoderelation_output.csv")
    // fields: 0:sourceid,1:sourceversion,2:sortorder,3:targetid,4:nodetype
    for (row <- nar) {
      val eid = row(3).toInt
      if (!editions.contains(eid)) logger.warn("Asset "+row.head+" links to non-edition node "+eid)
      else assetsToEditions.getOrElseUpdate(row.head, new mutable.HashSet[Int]) += eid
    }
    logger.info("Reading "+dir+"/assets_output.csv")
    val ar = parseCSV(dir+"/assets_output.csv")
    // fields: id,resourcetype,startdate,modifieddate,title,data,custom,timestamp,nodeid,body,splitbody
    for (row <- ar) if (row(1)=="article")
      articleInfo.put(row.head,ArticleInfo(row))
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
