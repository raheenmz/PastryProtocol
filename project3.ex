defmodule Project3 do
  def main(args) do
    args_list=elem(OptionParser.parse(args),1)
    numNodes=String.to_integer(Enum.at(args_list,0))
    numRequests=String.to_integer(Enum.at(args_list,1))
    
    Process.register(self(),:main)
    ### node creation and joining
    list_pid = createNodes(numNodes-1)
    IO.puts "Network created"    
    
    list_pid = joinNetwork(list_pid,numNodes)  
    IO.puts "New node joined successfully"

    ### sending request numbers
    total_request_count = send_request_number(list_pid,numRequests)
    IO.puts "Request count sent"

    total_hop = receive_hop_count(0,0, total_request_count )
    average_hops = total_hop/(total_request_count)
    IO.puts "Average hop count = "<>Float.to_string(average_hops)
  end

  def createNodes(numNodes) do
    list_pid=Enum.reduce(1..numNodes,[],fn (i,acc)->
                                                    nodeId=calHash(i)
                                                    pid=Pastry.start_link()
                                                    Process.register(elem(pid,1),String.to_atom(nodeId))
                                                    acc ++ [nodeId]
                                                    end)
    sorted_pid_list = Enum.sort(list_pid)
    Enum.map(sorted_pid_list, fn x -> Pastry.pastryInit(x,sorted_pid_list) end)
    list_pid
  end

  def joinNetwork(list_pid, i) do
    nodeId=calHash(i)
    pid=Pastry.start_link()
    Process.register(elem(pid,1),String.to_atom(nodeId))
    new_list_pid = list_pid ++ [nodeId]
    Pastry.pastryInit(nodeId,Enum.sort(new_list_pid))
    list_pid   
  end

  def calHash(i) do
    hash_256=:crypto.hash(:sha256,to_string(i)) |> Base.encode16
    hash=String.slice(hash_256,32..63)
    hash
  end

  def send_request_number(list_pid,request_count) do
    Enum.map(list_pid, fn(x) -> GenServer.cast( String.to_atom(x) , {:send_request_number,request_count,list_pid -- [x], x })  end) 
    length(list_pid)*request_count
  end

  def receive_hop_count(total, i, count) do
    receive do
      {:delivered,hop_count} -> 
                                total = total + hop_count  
                                
      if (i+1 < count) do
        total = receive_hop_count(total, i+1, count)
      end       
    end
     total
  end

end
