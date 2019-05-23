package adaptivecep.distributed.operator

import adaptivecep.data.Cost.Cost

import scala.concurrent.duration.Duration

object helper {
  def hostProps(costsMap: Map[Host, Map[Host, Cost]], hosts: Set[Host]): Map[Host, HostProps] = {
    var latencyStub: Seq[(Host, Duration)] = Seq.empty[(Host, Duration)]
    var bandwidthStub: Seq[(Host, Double)] = Seq.empty[(Host, Double)]
    hosts.foreach(host => {
      latencyStub = latencyStub :+ (host, Duration.Inf)
      bandwidthStub = bandwidthStub :+ (host, 0.0)
    })
    var result: Map[Host, HostProps] = Map.empty
    for(h <-hosts){
      var latencies: Seq[(Host, Duration)] = Seq.empty[(Host, Duration)]
      var dataRates: Seq[(Host, Double)] = Seq.empty[(Host, Double)]
      costsMap.foreach(host => {
        if(!host._2.contains(h)){
          latencies = latencies :+ (host._1, Duration.Zero)
        }
        else if(host._1 != host._2(h)){
          latencies = latencies :+ (host._1, host._2(h).duration)
        }}
      )
      costsMap.foreach(host =>{
        if(!host._2.contains(h)){
          dataRates = dataRates :+ (host._1, 10000.0)
        }
        else if(host._1 != host._2(h)){
          dataRates = dataRates :+ (host._1, host._2(h).bandwidth)
        }}
      )
      result += h -> HostProps(latencies, dataRates)
    }
    result += NoHost -> HostProps(latencyStub, bandwidthStub)
    result
  }
}
