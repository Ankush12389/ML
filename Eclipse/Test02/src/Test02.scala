
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
  type Data = Vector[Map[String, String]]
  type Domain = Map[String, Set[String]]
  

  def calcEntropy( x: Data, domain: Domain) = {
    val tgtValueProb = 
      x.groupBy(_(CU.Target)).map( x1 => x1._1 -> x1._2.size*1.0/x.size)
    -tgtValueProb.values.map( x => if( x < 1e-8 ) 0 else  {x * math.log(x)/math.log(2.0)}).sum
  }
  
  def scrapDomain( x: Data, domain: Domain, d: String ) = {
    val ret = x.groupBy(_(d))
    (ret, domain-d)
  }
  
  def maxEntropy( x: Data, domain: Domain ) = {
    (  domain - CU.Target ).map {
      d => 
        d._1 -> {
          val (mapDomainKeyXRows,newDomain) = scrapDomain(x, domain,d._1)
          val mapDomainKeyXProb = mapDomainKeyXRows.map( x1 => x1._1 -> x1._2.size * 1.0 / x.size )
          val mapDomainKeyXSE = mapDomainKeyXRows.map( x1 => x1._1 -> calcEntropy (x1._2, newDomain) )
          (mapDomainKeyXRows,newDomain,mapDomainKeyXProb,mapDomainKeyXSE.map( x => mapDomainKeyXProb(x._1)*x._2).sum)
        }
    }
  }
  
  def calcDecisionTree( data: Data, domain: Domain,tgtValueProb: Map[String,Double] ): DecisionTree = {
    
    if(domain.size==2) {
      val prop =   (domain-CU.Target).head._1
      val (mapDomainKeyXRows,newDomain) = scrapDomain(data, domain, prop)
      
      val leaves = 
        mapDomainKeyXRows
        .map(
            x1 => 
            x1._1 -> 
            
              DecisionTreeLeaf( x1._2.groupBy(_(CU.Target)).map(x2 => x2._1 -> x2._2.size*1.0/x1._2.size )) 
        )
      DecisionTreeNode(
          prop, 
          leaves,
          tgtValueProb
      )
    } else { 
      val entropy = calcEntropy(data, domain)
      if(entropy<1e-6) {
        DecisionTreeLeaf( domain(CU.Target).map(x=>x -> 0.0).toMap + ( data.map(_(CU.Target)).head -> 1.0 ) )
      } else { 
        val (prop,(grouped,newDomain,groupedProb,_)) = 
          DecisionTreeUtil.maxEntropy(data, domain)
          .map( x1 => x1._1 -> (x1._2._1,x1._2._2,x1._2._3,(entropy - x1._2._4 )))
          .maxBy(_._2._4)

        DecisionTreeNode(prop, grouped.map( x1 => x1._1 -> calcDecisionTree(x1._2, newDomain,groupedProb)),tgtValueProb)
      }
    }
     
  }
  
}

object ClassificationUtil {
  def getDomain( x: Vector[Map[String, String]] ) = {
    x.flatten.groupBy(_._1).map( x1 => x1._1 -> x1._2.map(_._2).toSet)
  }
  
  def updateTargetTitle(  x: Vector[Map[String, String]], tgt: String ) = {
    x.map( m =>  { m - tgt + ( CU.Target -> m(tgt) ) } )   
  }
  
  def updateBinValues(  x: Vector[Map[String, String]], key: String, f: String => String ) = {
    x.map( m =>  { m - key + ( key -> f(m(key)) ) } )   
  }
  
  def getBinFunc(minVal: Double, maxVal: Double, nBins: Int) = {
    (x: String) => {
      val binRange = ( maxVal - minVal )/(nBins-1)
      ((x.toDouble/binRange).intValue()).toString
    }
  }
  
  val colList = "duration,age,balance,day,pdays,previous,campaign".split(',')
  
  def bin(x: Vector[Map[String, String]], binKeys: Map[String, (Double,Double)], k1: Int) ={
    x.map( a => a++ binKeys.map( k => k._1 -> getBinFunc(k._2._1, k._2._2, k1)(a(k._1) ) ) )
  }
  
}

object Test02 {
  
  def binData(dataRaw: Vector[Map[String, String]], targetName: String, K:Int ) = {
    val data = ClassificationUtil.updateTargetTitle(dataRaw, targetName)
    val domain = ClassificationUtil.getDomain(data)
    val binKeys = (domain-CU.Target)
    .filter(x=>ClassificationUtil.colList.contains(x._1))
    .map( d => d._1 -> { val p = d._2.map(_.toDouble); (p.min, p.max) } )
    val binnedData = 
      
      ClassificationUtil.bin(data, binKeys,K)
    (binnedData, binKeys)
        
  }
  
  def main(args: Array[String]) = {
    
    var N = 2000
    var K = 5
    while (K<6) {
        var i = 1
        var countTrain =0.0;
        var countTest=0.0;
        var timings=0.0
        while(N<40000) {
          val dataRaw = scala.util.Random.shuffle(ReadCSV.readPath(args(0)).toList).toVector
          val targetName = args(2)
          val (binnedData, binKeys) = binData(dataRaw.take(N), targetName,K)
          val binnedDomain = ClassificationUtil.getDomain(binnedData)-"duration"
          val t1 = System.currentTimeMillis()
          val decisionTree = DecisionTreeUtil.calcDecisionTree(binnedData, binnedDomain,Map())
         // println(decisionTree)
          timings += System.currentTimeMillis()-t1 
      
          {
            val dataTestRaw = ClassificationUtil.updateTargetTitle(dataRaw.drop(N), targetName)
            val dataTest = ClassificationUtil.bin(dataTestRaw, binKeys,K)
            val resultTest = dataTest.map( x =>   
                x -> (decisionTree.findResult(x)+("NA"->0.0)).maxBy(_._2)._1  
              )
            val errorTest = resultTest.filterNot( x=> x._1(CU.Target)==x._2 ).map(x => {;x}).size * 1.0/dataTest.size
            countTest=countTest+(errorTest*100)
          }
          {
            val resultTrain = binnedData.map( x =>   
                x -> (decisionTree.findResult(x)+("NA"->0.0)).maxBy(_._2)._1  
              )
            val errorTrain = resultTrain.filterNot( x=> x._1(CU.Target)==x._2 ).map(x => {x}).size * 1.0/binnedData.size
            countTrain=countTrain+(errorTrain*100)
          }
          N+=2000
          
          println(N+","+countTrain/10.0+","+countTest/10.0+","+timings)
        }
        
        
        K=K+1
    } 
  }
}