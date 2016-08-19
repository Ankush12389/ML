
object ReadCSV {
  def readPath( file: String ) = {
    val csvLines: Iterator[Array[String]] = io.Source.fromFile(file).getLines.map(_.split(','))
    val titles = csvLines.next
    
    val ret = collection.mutable.ArrayBuffer[Map[String, String]]()
    
    while( csvLines.hasNext ){
      val values = csvLines.next()
      ret += ( titles zip values ).toMap
    }
    ret
  }
}

abstract class DecisionTree {
  def toString1( level: Int ): Unit
  override def toString() = {
    toString1(0)
    ""
  }
  
  def findResult( inputs: Map[String, String] ): Map[String, Double]
}

case class DecisionTreeNode(rootKey: String, children: Map[String,DecisionTree], tgtValueProb: Map[String, Double]) extends DecisionTree {
  def toString1(level: Int): Unit = {
    Range(0,level,1).foreach( _ => print(CU.D))
    println(rootKey+":")
      children.filterNot(_._2.isInstanceOf[DecisionTreeNA]).foreach( 
          x => {
            Range(0,level+1,1).foreach( _ => print(CU.D))
            println(x._1+"->")
            
            x._2.toString1(level+2)
          }
      )
  }
  
  
  
  def findResult(inputs: Map[String, String] ): Map[String, Double] = {
    children.get(inputs(rootKey)).map(_.findResult(inputs)).getOrElse(tgtValueProb)
  }
}
case class DecisionTreeLeaf(val children: Map[String, Double] ) extends DecisionTree {
  def toString1(level: Int): Unit = {
    if(children.filterNot(_._2<1e-8).size==1) {
      Range(0,level+1,1).foreach( _ => print(CU.D))
      println(children.filterNot(_._2<1e-8).head._1)
    } else {
      children.foreach( 
          x => {
            Range(0,level+1,1).foreach( _ => print(CU.D))
            println(x)
          }
      )
    }
  }

  def findResult(inputs: Map[String, String] ): Map[String, Double] = 
    children

}
case class DecisionTreeNA() extends DecisionTree {
  def toString1(level: Int): Unit = {}
  def findResult(inputs: Map[String, String] ): Map[String, Double] =
     Map()
}


object CU {
  val Target = "Target"
  val D = "   "
}

object DecisionTreeUtil {
  type Data = Iterable[Map[String, String]]
  type Domain = Map[String, Set[String]]
  
  def calcProb(x: Data, dKey: String, dValue: String ) = {
    x.filter(_(dKey)==dValue).size*1.0 / x.size  
  }
  
  def calcEntropy( x: Data, domain: Domain) = {
    val tgtValueProb = 
      domain(CU.Target).map{
        d => d-> calcProb(x,CU.Target,d)
      }.toMap
    -tgtValueProb.values.map( x => if( x < 1e-8 ) 0 else  {x * math.log(x)/math.log(2.0)}).sum
  }
  
  def scrapDomain( x: Data, domain: Domain, d: String ) = {
    domain(d).map {
      v => {
        v -> x.filter(_(d)==v).map( _ - d )
      }
    }.toMap
  }
  
  def maxEntropy( x: Data, domain: Domain ) = {
    (  domain - CU.Target ).map {
      d => 
        d._1 -> {
          val mapDomainKeyXRows = scrapDomain(x, domain,d._1)
          val mapDomainKeyXProb = mapDomainKeyXRows.map( x1 => x1._1 -> x1._2.size * 1.0 / x.size )
          val mapDomainKeyXSE = mapDomainKeyXRows.map( x1 => x1._1 -> calcEntropy (x1._2, domain) )
          mapDomainKeyXSE.map( x => mapDomainKeyXProb(x._1)*x._2).sum
        }
    }
  }
  
  def calcDecisionTree( data: Data, domain: Domain ): DecisionTree = {
            val tgtValueProb = 
      domain(CU.Target).map{
        d => d-> calcProb(data,CU.Target,d)
      }.toMap
    
    if(domain.size==2) {
      val prop =   (domain-CU.Target).head._1
      val mapDomainKeyXRows = scrapDomain(data, domain, prop)
      
      val leaves = 
        mapDomainKeyXRows
        .map(
            x1 => 
            x1._1 -> 
            
              domain(CU.Target).map(d => d -> x1._2.filter(_(CU.Target)==d ).size ).toMap 
        )
      DecisionTreeNode(
          prop, 
          leaves.filter(_._2.values.sum>0)map( 
              x => 
                x._1 -> {
                  val tuplesForResult = x._2.values.sum*1.0
                  if(tuplesForResult > 0 ) 
                    DecisionTreeLeaf( x._2.toMap.map( x1 => x1._1 -> x1._2/tuplesForResult ) ) 
                  else DecisionTreeNA() 
                } 
          ),
          tgtValueProb
      )
    } else { 
      val entropy = calcEntropy(data, domain)
      val prop = 
        DecisionTreeUtil.maxEntropy(data, domain)
        .map( x1 => x1._1 -> (entropy - x1._2 ))
        .maxBy(_._2)._1
      if(entropy==0) {
        DecisionTreeLeaf( domain(CU.Target).map(x=>x -> 0.0).toMap + ( data.map(_(CU.Target)).head -> 1.0 ) )
      } else { 
        val removeProp = scrapDomain( data, domain, prop )

        DecisionTreeNode(prop, removeProp.filter(_._2.size>0).map( x1 => x1._1 -> calcDecisionTree(x1._2, domain - prop)),tgtValueProb)
      }
    }
     
  }
  
}

object ClassificationUtil {
  def getDomain( x: Iterable[Map[String, String]] ) = {
    x.flatten.groupBy(_._1).map( x1 => x1._1 -> x1._2.map(_._2).toSet)
  }
  
  def updateTargetTitle(  x: Iterable[Map[String, String]], tgt: String ) = {
    x.map( m =>  { m - tgt + ( CU.Target -> m(tgt) ) } )   
  }
  
  def updateBinValues(  x: Iterable[Map[String, String]], key: String, f: String => String ) = {
    x.map( m =>  { m - key + ( key -> f(m(key)) ) } )   
  }
  
  def getBinFunc(minVal: Double, maxVal: Double, nBins: Int) = {
    (x: String) => {
      val binRange = ( maxVal - minVal )/(nBins-1)
      ((x.toDouble/binRange).intValue()).toString
    }
  }
  
  def bin(x: Iterable[Map[String, String]], binKeys: Map[String, (Double,Double)], k1: Int) ={
    x.map( a => a++ binKeys.map( k => k._1 -> getBinFunc(k._2._1, k._2._2, k1)(a(k._1) ) ) )
  }
  
}

object TestApp {
  
  def binData(dataRaw: Array[Map[String, String]], targetName: String, K:Int ) = {
    val data = ClassificationUtil.updateTargetTitle(dataRaw, targetName)
    val domain = ClassificationUtil.getDomain(data)
    val binKeys = (domain-CU.Target).map( d => d._1 -> { val p = d._2.map(_.toDouble); (p.min, p.max) } )
    val binnedData = ClassificationUtil.bin(data, binKeys,K)
    (binnedData, binKeys)
        
  }
  
  def main(args: Array[String]) = {
    var N = 75
    var K = 51
    while (K<100) {
        var i = 10
        var countTrain =0.0;
        var countTest=0.0;
        while(i>0) {
          val dataRaw = scala.util.Random.shuffle(ReadCSV.readPath(args(0)).toList).toArray
          val targetName = args(2)
          val (binnedData, binKeys) = binData(dataRaw.take(N), targetName,K)
          val binnedDomain = ClassificationUtil.getDomain(binnedData)
          val decisionTree = DecisionTreeUtil.calcDecisionTree(binnedData, binnedDomain)
      
          {
            val dataTestRaw = ClassificationUtil.updateTargetTitle(dataRaw.drop(N), targetName)
            val dataTest = ClassificationUtil.bin(dataTestRaw, binKeys,K)
            val resultTest = dataTest.map( x =>   
                x -> decisionTree.findResult(x).maxBy(_._2)._1  
              )
            val errorTest = resultTest.filterNot( x=> x._1(CU.Target)==x._2 ).map(x => {;x}).size * 1.0/dataTest.size
            countTest=countTest+(errorTest*100)
          }
          {
            val resultTrain = binnedData.map( x =>   
                x -> decisionTree.findResult(x).maxBy(_._2)._1  
              )
            val errorTrain = resultTrain.filterNot( x=> x._1(CU.Target)==x._2 ).map(x => {x}).size * 1.0/binnedData.size
            countTrain=countTrain+(errorTrain*100)
          }
          i=i-1
        }
        
        println(K+","+countTrain/10.0+","+countTest/10.0)
        K=K+1
    } 
  }
}