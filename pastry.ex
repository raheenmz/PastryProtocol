defmodule Pastry do
    use GenServer

    def start_link() do
        GenServer.start(__MODULE__,[]) 
    end

    def init([]) do
        leafSet=[]
        routingTable=[  [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil], 
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil],
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil], 
                        [nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil,nil] ]
        {:ok,[leafSet,routingTable]}
    end

    def pastryInit(pid, sorted_list) do

        # calculate leafSet
        curr_index = Enum.find_index(sorted_list, fn x -> x == pid end)
        min_bound = max(curr_index-8,0)
        max_bound = min(curr_index+8,length(sorted_list)-1)
        leafSet =  Enum.slice(sorted_list,min_bound..max_bound) -- [Enum.at(sorted_list,curr_index) ] 
        newLeafs(pid,leafSet)
        routing_table = calculateRoutingTable(pid, sorted_list )
        newRoutingTable(pid,routing_table)
    end

    def calculateRoutingTable(pid,sorted_list) do

        id_list = sorted_list -- [pid]
        routing_table = Enum.reduce(0..31,[],fn (i,acc)->
                                            prefix = String.slice(pid,0,i)
                                            row = calculateRow(id_list,prefix)
                                            acc ++ [row]
                                            end)
        routing_table
    end

    def calculateRow(id_list,prefix) do
        hex_list = ["0","1","2","3","4","5","6","7","8","9","a","b","c","d","e","f" ]
        row = Enum.map(hex_list, fn x -> Enum.find(id_list, fn y -> String.starts_with?(y,prefix <> x) end)  end)
        row
    end

    def newLeafs(pid,leafSet) do
        GenServer.cast(String.to_atom(pid),{:newLeafs,leafSet})
    end

    def newRoutingTable(pid,routing_table) do
        GenServer.cast(String.to_atom(pid),{:newRoutingTable,routing_table})
    end

    def viewState(pid) do
        GenServer.cast(String.to_atom(pid),{:viewState})
    end

    def handle_cast({:viewState},state) do 
        IO.inspect "cast of view state"
        IO.inspect ["state",state]
        {:noreply,state}
    end

    def handle_cast({:forward,message,key,curr_hop_count},state) do
        self_key = Atom.to_string(Process.info(self()) |> Enum.at(0) |> elem(1))
        route(message,key,self_key,curr_hop_count,state)
        {:noreply,state}
    end

    def handle_cast({:newLeafs,leafSet},state) do
        state = List.replace_at(state,0,leafSet)
        {:noreply,state}
    end

    def handle_cast({:newRoutingTable,routing_table},state) do
        state =  List.replace_at(state,1,routing_table)
        {:noreply,state}
    end

    def handle_cast({:send_request_number,request_count,list_pid,x},state) do
        # calls send messages whoch takes request count as parameter
        send_messages(request_count,list_pid,x,state) 
        {:noreply,state}
    end

    def handle_cast({:startJoin,known_node,node_id},state) do
        GenServer.cast(known_node |> String.to_atom(), {:joinMessage,node_id,0} )
        {:noreply,state}
    end

    def handle_cast({:joinMessage,node_id,counter},state) do
            GenServer.cast( node_id |> String.to_atom(), {:myStateTable,state,counter})
            self_key = Atom.to_string(Process.info(self()) |> Enum.at(0) |> elem(1))
            x =  find_next_id(node_id, self_key, state)
            if x == self_key or diff_key(x,node_id) > diff_key(self_key,node_id)  do
                GenServer.cast( node_id |> String.to_atom(), {:myStateTable,state,3000} )
            else
                forward_join(x,node_id,counter+1) 
            end
        
        updateSelfState(node_id,state)  
        {:noreply,state}
    end

    def handle_cast( {:arrival_message,nodeId} ,state) do
       state1 =  List.flatten(state)     
       new_list = Enum.filter(state1, & !is_nil(&1))
       Enum.map(new_list, fn x -> GenServer.call(  x |> String.to_atom(), {:newNodeJoined, nodeId},:infinity )  end)
       send(:main,{:broadcasted})
       {:noreply,state}
    end

    def handle_call( {:newNodeJoined, nodeId}, _from ,state) do
        updateSelfState(nodeId,state)
        {:reply,state,state}
     end


    def forward_join(next_id,node_id,counter) do
        GenServer.cast(next_id |> String.to_atom(), {:joinMessage,node_id,counter})
    end

    def handle_cast(  {:myStateTable,neigh_state,counter},state)   do
        self_key = Atom.to_string(Process.info(self()) |> Enum.at(0) |> elem(1))
        if counter == 3000 do 
            new_leaf_set = Enum.at(neigh_state,0) 
            newLeafs(self_key, new_leaf_set)
            send(:main,{:ok})
        else
            self_routing_table = Enum.at(state,1)
            row_num = counter
            routing_table = Enum.at(neigh_state,1)
            new_row = Enum.at(routing_table, row_num) 
            new_table = List.replace_at(self_routing_table,row_num, new_row)
            newRoutingTable(self_key,new_table)
        end

        {:noreply,state}
    end

    def updateSelfState(node_id,state) do
        # check for leaf node
        updateLeafSet(node_id,state)

        # check for routing table
        updateRoutingTable(node_id,state)
    
    end

    def updateRoutingTable(node_id,state) do
        self_key = Atom.to_string(Process.info(self()) |> Enum.at(0) |> elem(1))
        routing_table = Enum.at(state,1)
        hex_list = ["0","1","2","3","4","5","6","7","8","9","A","B","C","D","E","F" ]
        row_num = common_prefix_length(node_id,self_key) 
        col_num = Enum.find_index(hex_list, fn x -> String.at(node_id,row_num) == x  end ) 
        if !(Enum.at(Enum.at(routing_table, row_num),col_num))  do ## check if entry at the row at column Dl is not null 
            new_row =  List.replace_at(Enum.at(routing_table, row_num),col_num,node_id)
            new_table = List.replace_at(routing_table,row_num, new_row)
        end
        newRoutingTable(self_key,new_table)
    end

    def updateLeafSet(node_id,state) do
        leaf_set = Enum.at(state,0)
        self_key = Atom.to_string(Process.info(self()) |> Enum.at(0) |> elem(1))
        leaf_set = [self_key|leaf_set]
        leaf_set = Enum.sort(leaf_set)
        node_ind = Enum.find_index(leaf_set, fn x -> x == self_key end)
        {smaller_set,larger_set}= Enum.split(leaf_set, node_ind)        
        larger_set = larger_set -- [self_key]

        if self_key > node_id do
            if length(smaller_set) < 8 do
                smaller_set = smaller_set ++ [node_id]
                smaller_set = Enum.sort(smaller_set)
            else
                if node_id > Enum.at(smaller_set,0) do
                    smaller_set = List.delete_at(smaller_set,0)
                    smaller_set = smaller_set ++ [node_id]
                    smaller_set = Enum.sort(smaller_set)
                end  
            end
        end

        if self_key < node_id do
            if length(larger_set) < 8 do
                larger_set = larger_set ++ [node_id]
                larger_set = Enum.sort(larger_set)
            else
                if node_id < Enum.at(larger_set,length(larger_set)-1) do
                    larger_set = List.delete_at(larger_set, length(larger_set)-1 )
                    larger_set = larger_set ++ [node_id]
                    larger_set = Enum.sort(larger_set)
                end  
            end
        end

    leaf_set = smaller_set ++ larger_set

    newLeafs(self_key,leaf_set)
    end
    
    def send_messages(request_count,list_pid,x,state) do
        len =length(list_pid)

        for i <- 1..request_count do
            Process.sleep(1000)
            key = select_random_node(list_pid, len) #select random node
            message = x<>" sends "<>key<>" "
            route(message,key,x,0,state) #send message to node
        end
    end


    def select_random_node(node_pid_list, len) do
        Enum.at(node_pid_list,:rand.uniform(len)-1)        
    end

    def route(message,key,x,curr_hop_count,state) do
        if key == x do
            deliver(curr_hop_count)
        else
            next_id = find_next_id(key,x,state) ## finds nearest node
            if next_id == x do
                deliver(curr_hop_count+1)
            else
                forward(message,key,next_id,curr_hop_count+1) ## forwards to nearest node
            end
        end
    end

    def deliver(hop_count) do
        send(:main, {:delivered,hop_count})
    end

    def find_next_id(key,x,state) do 
    
        leaf_set = Enum.at(state,0)
    
        if List.first(leaf_set) <= key and List.last(leaf_set) >= key do ## checking if key is part of leaf set
            leaf_set_search(leaf_set,key) ## finds and returns min
        else
            hex_list = ["0","1","2","3","4","5","6","7","8","9","A","B","C","D","E","F" ]
            routing_table = Enum.at(state,1)
            row_num = common_prefix_length(key,x) 
            col_num = Enum.find_index(hex_list, fn x -> String.at(key,row_num) == x  end ) 
            #IO.inspect [row_num,col_num]
            if (Enum.at(Enum.at(routing_table, row_num),col_num))  do ## check if entry at the row at column Dl is not null 
                Enum.at(Enum.at(routing_table, row_num),col_num)
            else
                routing_list = Enum.at(routing_table,row_num)
                new_list = Enum.filter(routing_list, & !is_nil(&1))
                total_set = leaf_set ++ new_list

                result = leaf_set_search(total_set,key)

                if ( diff_key(key, result) > diff_key(key,x) ) do
                    result   
                else
                    x
                end

            end

        end    
    end

    def leaf_set_search(leaf_set,key) do
        new_set = leaf_set 
        key_int = elem(Integer.parse(key,16),0)
        leaf_set_int = Enum.map(new_set, fn(x) -> abs(elem(Integer.parse(x,16),0) - key_int) end )
        closest_key = Enum.min(leaf_set_int)
        index = Enum.find_index(leaf_set_int, fn x -> x == closest_key end)                
        Enum.at(leaf_set,index) 

    end
    
    def joinProcess(nodeId,known_node) do
        GenServer.cast( nodeId |> String.to_atom(), {:startJoin,known_node,nodeId} ) ;
        receive do
          {:ok} -> :ok
        end
    end


    def diff_key(key1,key2) do
        abs(elem(Integer.parse(key1,16),0) - elem(Integer.parse(key2,16),0))
    end
    
    def common_prefix_length(key, self_key) do
        keyword_list = String.myers_difference(key,self_key)
        first_tuple = hd(keyword_list)
        if elem(first_tuple,0) != :eq do  
            0
        else
            common_prefix = Keyword.get(keyword_list, :eq)
            len = String.length(common_prefix)
            len
        end
    end
    
    def forward(message,key,next_id,curr_hop_count) do
        GenServer.cast(String.to_atom(next_id),{:forward,message<>" "<>next_id,key,curr_hop_count})    
    end

end
