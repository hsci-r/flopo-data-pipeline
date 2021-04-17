import com.github.tototoshi.csv.CSVReader
import fi.hsci.{FluidDocument, OctavoIndexer}
import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.analysis.tokenattributes.{CharTermAttribute, OffsetAttribute, PositionIncrementAttribute}
import org.apache.lucene.document.StoredField
import org.apache.lucene.index.FieldInfo
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search.{Sort, SortField}
import org.apache.lucene.util.BytesRef
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import org.json4s.{JField, JNothing, JObject, JString, JValue}

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.collection.mutable.ArrayBuffer

object CONLLCSVOctavoIndexer extends OctavoIndexer {

  private var indexCONLL: Boolean = _
  private var indexPunctuation: Boolean = _
  private var hasDocumentParts: Boolean = _
  private var titlePart: Option[String] = _

  private val tld = new ThreadLocal[Reuse] {
    override def initialValue() = new Reuse()
  }

  private class Reuse {
    val sd = new FluidDocument()
    val pd = new FluidDocument()
    val dpd = new FluidDocument()
    val ad = new FluidDocument()
    val documentIDFields = new StringSDVFieldPair("document_id").r(sd, pd, dpd, ad)
    val documentPartIdFields = if (hasDocumentParts) new StringNDVFieldPair("document_part_id").r(sd, pd, dpd) else null
    val paragraphIDFields = new StringNDVFieldPair("paragraph_id").r(sd, pd)
    val sentenceIDFields = new StringNDVFieldPair("sentence_id").r(sd)
    val documentPartsFields = if (hasDocumentParts) new IntPointNDVFieldPair("document_parts").r(ad) else null
    val paragraphsFields = new IntPointNDVFieldPair("paragraphs").r(dpd, ad)
    val sentencesFields = new IntPointNDVFieldPair("sentences").r(pd, dpd, ad)
    val contentLengthFields = new IntPointNDVFieldPair("nr_characters").r(sd, pd, dpd, ad)
    val contentTokensFields = new IntPointNDVFieldPair("nr_tokens").r(sd, pd, dpd, ad)
    val titleFields = if (titlePart.isDefined) new TextSDVFieldPair("title").r(ad) else null

    val documentPartFields = if (hasDocumentParts) new StringSDVFieldPair("document_part").r(sd, pd, dpd) else null
    val textField = new ContentField("text").r(sd, pd, dpd, ad)
    val conllField = if (indexCONLL) new FieldWrapper(new StoredField("conll","")).r(sd, pd, dpd, ad) else null

    /*def clearMultiOptionalDocumentFields() {
      ad.clearOptional()
      pd.clearOptional()
      dpd.clearOptional()
      sd.clearOptional()
    }*/
  }

  val termVectorFields = Seq("text")

  case class TokenInfo(
                      documentId: String,
                      documentPart: String,
                       paragraphId: Int,
                       sentenceId: Int,
                       wordId: Int,
                       lcword: String,
                       lemma: String,
                       upos: String,
                       xpos: String,
                       feats: Seq[String],
                       head: Int,
                       deprel: String,
                       whitespaceBefore: String,
                       token: String,
                       whitespaceAfter: String,
                      ) {
    def analyses: Iterator[String] = {
      val a = new Array[String](feats.length + 5)
      a(0)="W="+token
      a(1)="L="+lemma
      a(2)="U="+upos
      a(3)="X="+xpos
      a(4)="D="+deprel
      feats.map("F"+_).copyToArray(a,5)
      a.iterator
    }
    def asJson: JObject =
      ("whitespace_before"->(if (whitespaceBefore=="") JNothing else JString(whitespaceBefore))) ~
      ("token"->token) ~
      ("whitespace_after"->(if (whitespaceAfter==" ") JNothing else JString(whitespaceAfter))) ~
      ("lemma"->lemma) ~
      ("xpos"->xpos) ~
      ("upos"->upos) ~
      ("feats"->(if (feats.isEmpty) JNothing else JObject(feats.map(f => {
        val split = f.indexOf('=')
        JField(f.substring(0,split),JString(f.substring(split+1)))
      }).toList))) ~
      ("head"->head) ~
      ("deprel"->deprel)
}

  object TokenInfo {
    private def mapWhitespace(tokenIn: String, misc: String): (String,String,String) = if (misc=="") ("",tokenIn," ") else {
      var before = ""
      var after = " "
      var token = tokenIn
      for ((position,whitespace) <- misc.split('|').map(t => {
        val split = t.indexOf('=')
        if (split == -1) throw new IllegalArgumentException(t)
        val position = t.substring(0,split)
        val whitespace = t.substring(split+1).replaceAll("\\\\n","\n").replaceAll("\\\\s"," ")
        (position,whitespace)
      }))
        position match {
          case "SpaceAfter" if whitespace!="No" => throw new IllegalArgumentException(s"SpaceAfter encountered but value is not No: $whitespace")
          case "SpaceAfter"  => after=""
          case "SpacesAfter" => after=whitespace
          case "SpacesBefore" => before=whitespace
          case "SpacesInToken" => token=whitespace
          case _ =>
        }
      (before,token,after)
    }
    def apply(conllCSV: Array[String]): TokenInfo = {
      //documentId,paragraphId,sentenceId,wordId,word,lemma,upos,xpos,feats,head,deprel,misc
      val rdocumentId = conllCSV.head
      val split = rdocumentId.indexOf('_')
      val (documentId,documentPart) = if (split != -1) (rdocumentId.substring(0,split),rdocumentId.substring(split+1)) else (rdocumentId,"")
      val paragraphId = conllCSV(1).toInt
      val sentenceId = conllCSV(2).toInt
      val wordId = conllCSV(3).toInt
      val (whitespaceBefore,token,whitespaceAfter) = mapWhitespace(conllCSV(4),conllCSV(11))
      val lcword = conllCSV(4).toLowerCase()
      val lemma = conllCSV(5)
      val upos = conllCSV(6)
      val xpos = conllCSV(7)
      val feats: Seq[String] = if (conllCSV(8)=="") Seq.empty[String] else conllCSV(8).split('|')
      val head = conllCSV(9).toInt
      val deprel = conllCSV(10)
      TokenInfo(documentId,documentPart,paragraphId,sentenceId,wordId,lcword,lemma,upos,xpos,feats,head,deprel,whitespaceBefore,token,whitespaceAfter)
    }
  }

  private class CONLLCSVAnalysisTokenStream(tokens: Iterable[TokenInfo], skipPunctuation: Boolean) extends TokenStream {

    private val termAttr: CharTermAttribute = addAttribute(classOf[CharTermAttribute])
    private val posAttr: PositionIncrementAttribute = addAttribute(classOf[PositionIncrementAttribute])
    private val offAttr: OffsetAttribute = addAttribute(classOf[OffsetAttribute])

    private var wordsIterator: Iterator[TokenInfo] = _

    override def reset(): Unit = {
      wordsIterator = tokens.iterator
      analysesIterator = Iterator.empty
    }

    private var analysesIterator: Iterator[String] = _

    private var startOffset = 0
    private var lastOffsetIncr = 0
    private var endOffset = 0

    final override def incrementToken(): Boolean = {
      clearAttributes()
      val analysisToken = if (!analysesIterator.hasNext) { // end of analyses
        if (!wordsIterator.hasNext) return false // end of words
        var t = wordsIterator.next
        if (skipPunctuation) while (t.xpos=="U=PUNCT") {
          lastOffsetIncr += t.whitespaceBefore.length + t.token.length + t.whitespaceAfter.length
          if (!wordsIterator.hasNext) return false // end of words
          t = wordsIterator.next
        }
        posAttr.setPositionIncrement(1)
        startOffset = endOffset + lastOffsetIncr + t.whitespaceBefore.length
        endOffset = startOffset + t.token.length
        lastOffsetIncr = t.whitespaceAfter.length
        analysesIterator = t.analyses
        t.lcword
      } else {
        posAttr.setPositionIncrement(0)
        analysesIterator.next // next analysis
      }
      offAttr.setOffset(startOffset, endOffset)
      termAttr.append(analysisToken)
      true
    }
  }

  val processed = new AtomicInteger(0)

  val documentParts = new AtomicLong(0)
  val paragraphs = new AtomicLong(0)
  val sentences = new AtomicLong(0)

  private def tokensToString(tokens: Iterable[TokenInfo]): String = {
    val content = new StringBuilder()
    for (token <- tokens) {
      content.append(token.whitespaceBefore)
      content.append(token.token)
      content.append(token.whitespaceAfter)
    }
    content.toString
  }

  private def orderedGroupBy[T, P](seq: collection.Seq[T])(f: T => P): ArrayBuffer[(P, ArrayBuffer[T])] = {
    val ret = new ArrayBuffer[(P,ArrayBuffer[T])]
    var cur = null.asInstanceOf[ArrayBuffer[T]]
    var lastKey = null.asInstanceOf[P]
    for (d <- seq) {
      val key = f(d)
      if (key!=lastKey) {
        if (lastKey!=null) ret += ((lastKey,cur))
        cur = new ArrayBuffer[T]
        lastKey = key
      }
      cur += d
    }
    if (lastKey!=null)  ret += ((lastKey,cur))
    ret
  }

  private def processTokensIntoFields(contentBuilder: StringBuilder, itokens: ArrayBuffer[TokenInfo], r: Reuse): Unit = {
    var bindex = 0
    var eindex = contentBuilder.length
    var tokens: ArrayBuffer[TokenInfo] = itokens
    if (tokens.head.whitespaceBefore != "") {
      bindex = tokens.head.whitespaceBefore.length
      tokens = itokens.clone()
      tokens(0) = tokens.head.copy(whitespaceBefore = "")
    }
    if (tokens.last.whitespaceAfter != "") {
      eindex -= tokens.last.whitespaceAfter.length
      if (itokens.head.whitespaceBefore == "") tokens = itokens.clone()
      tokens(tokens.length-1) = tokens.last.copy(whitespaceAfter = "")
    }
    val content = contentBuilder.substring(bindex,eindex)
    r.textField.setValue(content,new CONLLCSVAnalysisTokenStream(tokens,skipPunctuation = !indexPunctuation))
    r.contentTokensFields.setValue(tokens.length)
    r.contentLengthFields.setValue(content.length)
  }

  private def processSentenceTokensIntoFields(itokens: ArrayBuffer[TokenInfo], r: Reuse): String = {
    val icontent = tokensToString(itokens)
    var bindex = 0
    var eindex = icontent.length
    var tokens: ArrayBuffer[TokenInfo] = itokens
    if (tokens.head.whitespaceBefore != "") {
      bindex = tokens.head.whitespaceBefore.length
      tokens = itokens.clone()
      tokens(0) = tokens.head.copy(whitespaceBefore = "")
    }
    if (tokens.last.whitespaceAfter != "") {
      eindex -= tokens.last.whitespaceAfter.length
      if (itokens.head.whitespaceBefore == "") tokens = itokens.clone()
      tokens(tokens.length-1) = tokens.last.copy(whitespaceAfter = "")
    }
    val content = icontent.substring(bindex,eindex)
    r.textField.setValue(content,new CONLLCSVAnalysisTokenStream(tokens,skipPunctuation = !indexPunctuation))
    r.contentTokensFields.setValue(tokens.length)
    r.contentLengthFields.setValue(content.length)
    icontent
  }


  private def index(documentId: String, documentRows: ArrayBuffer[Array[String]]): Unit = {
    val seq = processed.incrementAndGet()
    if (seq%128==0) logger.info(f"Processing $documentId%s with ${documentRows.length}%,d tokens at ${durationToString(System.currentTimeMillis()-startTime)}%s. Processed $seq%,d documents, ${paragraphs.get}%,d paragraphs and ${sentences.get}%,d sentences (${1000*seq/(System.currentTimeMillis()-startTime)}%,d documents/s).")
    val r = tld.get
    r.documentIDFields.setValue(documentId)
    val idocumentTokens = documentRows.map(TokenInfo.apply)
    var titleTokens: scala.collection.Seq[TokenInfo] = Seq.empty[TokenInfo]
    var dsentences = 0
    var dparagraphs = 0
    val tokensByDocumentPart = orderedGroupBy(idocumentTokens)(_.documentPart)
    val documentTokens = new ArrayBuffer[TokenInfo]()
    val documentJson = new ArrayBuffer[JValue](tokensByDocumentPart.length)
    val documentContent = new StringBuilder()
    for ((documentPart,documentPartTokens) <- tokensByDocumentPart) {
      val documentPartContent = new StringBuilder()
      if (titlePart.contains(documentPart)) titleTokens = documentPartTokens
      if (hasDocumentParts) {
        r.documentPartIdFields.setValue(documentParts.incrementAndGet())
        r.documentPartFields.setValue(documentPart)
      }
      val tokensByParagraph = orderedGroupBy(documentPartTokens)(_.paragraphId)
      val documentPartJson = new ArrayBuffer[JValue](tokensByParagraph.length)
      var dpsentences = 0
      for ((_,paragraphTokens) <- tokensByParagraph) {
        val paragraphContent = new StringBuilder()
        r.paragraphIDFields.setValue(paragraphs.incrementAndGet())
        val tokensBySentence = orderedGroupBy(paragraphTokens)(_.sentenceId)
        val paragraphJson = new ArrayBuffer[JValue](tokensBySentence.length)
        for ((_,sentenceTokens) <- tokensBySentence)  {
          paragraphContent ++= processSentenceTokensIntoFields(sentenceTokens,r)
          r.sentenceIDFields.setValue(sentences.incrementAndGet())
          if (indexCONLL) {
            val json = sentenceTokens.map(_.asJson)
            paragraphJson += json
            r.conllField.setStringValue(compact(render(json)))
          }
          siw.addDocument(r.sd)
        }
        processTokensIntoFields(paragraphContent,paragraphTokens,r)
        r.sentencesFields.setValue(tokensBySentence.length)
        documentPartContent ++= paragraphContent
        dpsentences += tokensBySentence.length
        if (indexCONLL) {
          val json = paragraphJson
          documentPartJson += json
          r.conllField.setStringValue(compact(render(json)))
        }
        piw.addDocument(r.pd)
      }
      if (hasDocumentParts) {
        processTokensIntoFields(documentPartContent,documentPartTokens,r)
        r.paragraphsFields.setValue(tokensByParagraph.length)
        r.sentencesFields.setValue(dpsentences)
        if (indexCONLL) {
          documentJson += ("documentPart" -> "") ~ ("paragraphs" -> documentPartJson)
          r.conllField.setStringValue(compact(render(documentPartJson)))
        }
        dpiw.addDocument(r.dpd)
      } else if (indexCONLL)
        documentJson.addAll(documentPartJson)
      dsentences += dpsentences
      dparagraphs += tokensByParagraph.length
      documentTokens ++= (
          if (!documentPartTokens.last.whitespaceAfter.endsWith("\n\n")) {
            var nwhitespaceAfter = documentPartTokens.last.whitespaceAfter
            if (nwhitespaceAfter.endsWith("\n")) {
              documentPartContent += '\n'
              nwhitespaceAfter = nwhitespaceAfter + "\n"
            } else {
              documentPartContent ++= "\n\n"
              nwhitespaceAfter = nwhitespaceAfter + "\n\n"
            }
            documentPartTokens.dropRight(1) ++ Seq(documentPartTokens.last.copy(whitespaceAfter = nwhitespaceAfter))
          } else documentPartTokens
        )
      documentContent ++= documentPartContent
    }
    processTokensIntoFields(documentContent,documentTokens,r)
    r.sentencesFields.setValue(dsentences)
    r.paragraphsFields.setValue(dparagraphs)
    if (hasDocumentParts) r.documentPartsFields.setValue(tokensByDocumentPart.length)
    if (titlePart.isDefined)
      r.titleFields.setValue(tokensToString(titleTokens),new CONLLCSVAnalysisTokenStream(titleTokens,skipPunctuation = !indexPunctuation))
    if (indexCONLL)
      r.conllField.setStringValue(compact(render(documentJson)))
    aiw.addDocument(r.ad)
    //r.clearMultiOptionalDocumentFields()
  }

  var siw, piw, dpiw, aiw = null.asInstanceOf[IndexWriter]

  val aSort = new Sort(new SortField("document_id",SortField.Type.STRING))
  val dpSort = new Sort(new SortField("document_id",SortField.Type.STRING), new SortField("document_part_id", SortField.Type.LONG))
  var pSort = null.asInstanceOf[Sort]
  var sSort = null.asInstanceOf[Sort]

  def main(args: Array[String]): Unit = {
    val opts = new AOctavoOpts(args) {
      val indexPunctuation = opt[Boolean](default = Some(false))
      val indexConll = opt[Boolean](default = Some(false))
      val hasDocumentParts = opt[Boolean](default = Some(false))
      val termVectorOnlyLemmas = opt[Boolean](default = Some(false))
      val titlePart = opt[String]()
      verify()
    }
    if (opts.termVectorOnlyLemmas()) indexingCodec.termVectorFilter = (_: FieldInfo, b: BytesRef) => b.bytes(b.offset) == 'L' && b.length>1 && b.bytes(b.offset + 1) == '='
    indexPunctuation = opts.indexPunctuation()
    indexCONLL = opts.indexConll()
    hasDocumentParts = opts.hasDocumentParts()
    titlePart = opts.titlePart.toOption
    val source = opts.directories().head
    pSort = if (hasDocumentParts) new Sort(new SortField("document_id",SortField.Type.STRING), new SortField("document_part_id", SortField.Type.LONG), new SortField("paragraph_id", SortField.Type.LONG)) else new Sort(new SortField("document_id",SortField.Type.STRING), new SortField("paragraph_id", SortField.Type.LONG))
    sSort = if (hasDocumentParts) new Sort(new SortField("document_id",SortField.Type.STRING), new SortField("document_part_id", SortField.Type.LONG), new SortField("paragraph_id", SortField.Type.LONG), new SortField("sentence_id", SortField.Type.LONG)) else new Sort(new SortField("document_id",SortField.Type.STRING), new SortField("paragraph_id", SortField.Type.LONG), new SortField("sentence_id", SortField.Type.LONG))
    val md = if (hasDocumentParts) 4 else 3
    if (!opts.noIndex()) {
      siw = iw(opts.index() + "/sentence_index", sSort, opts.indexMemoryMb() / md, mmapped = !opts.noMmap())
      dpiw = if (hasDocumentParts) iw(opts.index() + "/document_part_index", dpSort, opts.indexMemoryMb() / md, mmapped = !opts.noMmap()) else null
      piw = iw(opts.index() + "/paragraph_index", pSort, opts.indexMemoryMb() / md, mmapped = !opts.noMmap())
      aiw = iw(opts.index() + "/document_index", aSort, opts.indexMemoryMb() / md, mmapped = !opts.noMmap())
      var rows = new ArrayBuffer[Array[String]]
      var lastDocumentId = ""
      feedAndProcessFedTasksInParallel(() => {
        val r = CSVReader.open(source)
        r.readNext()
        for (row <- r) {
          val split = row.head.indexOf('_')
          val newDocumentId = if (split == -1) row.head else row.head.substring(0, split)
          if (newDocumentId != lastDocumentId) {
            val documentId = lastDocumentId
            val documentRows = rows
            if (rows.nonEmpty) addTask(documentId, () => index(documentId, documentRows))
            rows = new ArrayBuffer[Array[String]]
            lastDocumentId = newDocumentId
          }
          rows += row.toArray
        }
        if (rows.nonEmpty) addTask(lastDocumentId, () => index(lastDocumentId, rows))
      })
    }
    waitForTasks(
      runInOtherThread(
        () => {
          if (!opts.noIndex()) close(siw)
          if (!opts.noMerge()) merge(opts.index() + "/sentence_index", sSort, opts.indexMemoryMb() / md, toCodec(termVectorFields), mmapped = !opts.noMmap())
        }
      ),
      runInOtherThread(
        () => {
          if (!opts.noIndex()) close(piw)
          if (!opts.noMerge()) merge(opts.index() + "/paragraph_index", pSort, opts.indexMemoryMb() / md, toCodec(termVectorFields), mmapped = !opts.noMmap())
        }
      ),
      runInOtherThread(
        () => if (hasDocumentParts) {
          if (!opts.noIndex()) close(dpiw)
          if (!opts.noMerge()) merge(opts.index() + "/document_part_index", dpSort, opts.indexMemoryMb() / md, toCodec(termVectorFields), mmapped = !opts.noMmap())
        }
      ),
      runInOtherThread(
        () => {
          if (!opts.noIndex()) close(aiw)
          if (!opts.noMerge()) merge(opts.index() + "/document_index", aSort, opts.indexMemoryMb() / md, toCodec(termVectorFields), mmapped = !opts.noMmap())
        }
      )
    )
  }
}
