{-

Simulation of Basic Paxos Consensus Protocol

Authors: Hemanth Kumar Mantri (HM7787), Makarand Damle (MSD872)
         mantri@cs.utexas.edu, mdamle@cs.utexas.edu
         Department of Computer Science
         The University of Texas at Austin

References: 1. Paxos Made Simple, Leslie Lamport
  	    2. ORC Reference Guide: http://orc.csres.utexas.edu/
            3. Professor Seif Haridi's Video Lectures on YouTube
            4. IIT Madras Video Lectures on YouTube
            5. http://the-paper-trail.org

Tags: distributed-consensus paxos
-}

{- type of messages -}
val PREP = "prepare"  		-- prepare message, (Proposer --> Acceptor)
val ACPT = "accept"  		-- accept message   (Proposer --> Acceptor)

val PRM  = "promise"  		-- promise message  (Acceptor --> Proposer)
val NACK = "negativeack"  	-- reject message   (Acceptor --> Proposer)
val ACPTD = "accepted"  	-- accepted message (Acceptor --> Proposer)

val DEC  = "decide"  		-- decision message (Proposer --> Client)
val CLNTREQ  = "clientreq"  -- client request	(Client --> Proposer)

val HBT  = "heartbeat"  	-- hearbeat message (Every Acceptor/Proposer --> Every Other Acceptor/Proposer)
val TMOUT = "timedout" 		-- timeout message  (Placeholder when Timeout occurs)
val REPN = "replication"	-- replication message	(Acceptor --> Listener)

{- roles of Nodes -}
val PROP = "proposer" 		-- proposes a value requested by client
val LIST = "listener" 		-- learn from acceptors, when they accept
val ACCP = "acceptor" 		-- reject a proposal or accept and send the accepted value to listeners
val CLNT = "client"   		-- suggests a value to the proposer

{- state of Proposer -}
val PHASE1 = "phase1" 	-- in phase 1
val PHASE2 = "phase2" 	-- in phase 2
val DCD = "decided" 	-- final state is decided
val RJT = "rejected" 	-- rejected in phase1 or phase 2
val INIT = "init"		-- initial stage
  
{- Global Utility Methods -}

def printChIds([]) = Println("")
def printChIds(ch:chs) = Print(ch.getId()) >> printChIds(chs)

-- methods needed to filter desired channels/processes from the list
-- of all channels and processes
val globalCurrent = Ref(-1)

def checkDst(src,dst) = 
	if (src = dst) 
		then false
	else (Ift(dst = globalCurrent?) >> true ) ; false
     
def checkSrc(src,dst) = 
	if (src = dst) 
		then false
	else (Ift(src = globalCurrent?) >> true ) ; false
	
def testChListener(ch) =
	ch.getRole()>role> (if (role? = LIST) then true else false)

def testChIn(ch) = 
	ch.getId() >(src,dst)> checkDst(src,dst)

def testChOut1(ch) = 
	ch.getId() >(src,dst)> checkSrc(src,dst)
	
def testZeroOut(ch) =
	ch.getId() >(src, dst)> (if (src = 0) then true else false)

def testChOut2(ch) = 
	ch.getRole()>role> (if (role? = LIST) then false 
	else (ch.getId() >(src, dst)> checkSrc(src,dst)))
   
def testListener(p)  = p.getRole() >role> (if (role? = LIST) then true else false)
def testAcceptor(p)  = p.getRole() >role> (if (role? = ACCP) then true else false)
def testProposer(p)  = p.getRole() >role> (if (role? = PROP) then true else false)
def testClient(p)    = p.getRole() >role> (if (role? = CLNT) then true else false)


{- Encapsulates Message Info -}
def class message(mtype, seq, data) = 
	val msgType = mtype
	val seqNum =  seq   -- current seqNum for PREP/ACPT, current/lastmaximum for PRM
	val value = data

	def getVal() = value
	def getType() = msgType
	def getSeq() = seqNum
stop

{- let the put fail with a probability 'p/100' to mimic a faultyChannel -}
def class faultyChannel(fail, id) =
  val chId = id
  val ch = Channel()
  val role = Ref(ACCP)
  val p = Ref(fail)
  
  def getFail() = p?
  def get() = ch.get()
  def put(x) = 
     if ((Random(99) + 1) :> p?) 
       then ch.put(x)
     else signal

  def getId() = chId
  def getRole() = role
  def setRole(r) = role := r
  def setFail(f) = p := f

  def printInfo() = Println("Channel:" +id+ "Role: "+role?)
stop

def class process(id, tOut, numNodes, role, in:ins, out:outs, lisnts) =

	{-- Generic Process State -}
	val role = Ref(role)
	val lock = Semaphore(1)
	val pid = id
	val maxDelay = 1
	val timeout = tOut
	-- nodes alive in my perspective
	val alive = fillArray(Array(numNodes), lambda(i) = true)
	val leader = Ref(0)
	
	{- Proposer State -}
	val PropHighestSeqNo = Ref(id)
	val Proplock = Semaphore(1)
	val PropNumPromised = Ref(0)
	val PropNumNotPromised = Ref(0)
	val PropNumAccepted = Ref(0)
	val PropNumRejected = Ref(0)
	val PropQuorumSize = Ref(numNodes/2)
	val PropState = Ref(INIT)
	--val PropQuorumSize = Ref(numNodes - 4)


	{- Acceptor State -}
	val AcpHighestSeqNo = Ref(id)
	val Acplock = Semaphore(1)
	
	{- Client State -}
	val clntReq = Ref(0)
	val clntServed = Ref(0)
	
	{- Listener State -}
	val lsnrStore = Ref(0)

	{- Generic Process Methods -}
	
	{- given an in-channel, get the corresponding out-channel
 	 - input: channel corresponding to (2,3)
 	 - output: channel corresponding to (3,2)
     -}
	def getOutch(ch, []) = stop
	def getOutch(ch, out:outCh) =
		ch.getId() >(src, dst)> 
	   	out.getId()>(a, b)> 
	   	(if(b = src)
	   		then out
	   	else getOutch(ch, outCh))

   -- send a message to all other nodes
	def broadcast(msg) =
   		map(lambda(ch) = Rwait(Random(maxDelay)) >> ch.put(msg), out:outs) >> signal
    
	-- propogate accepted value to the listeners to replicate
   	def sayListeners(msg) =
   		map(lambda(ch) = Rwait(Random(maxDelay)) >> ch.put(msg), lisnts) >> signal
   		
   	def getRole() = role
	def setRole(r) = (role := r) 
	def getId() = pid
	def myInChannels() = in:ins
	def myOutChannels() = out:outs
	def setPropState(s) = PropState := s
	
	{- client Methods -}
	{-
	def sendClientReq(v) =
		Println("Client (Process-"+pid+") requests the Leader (Process-"+getLeader(procList)+") for value:"+v) >>
		clntReq := v >> message(CLNTREQ, -1, v) >m> getLeaderCh(out:outs) >ch> ch.put(m)
		
	def recvConsensus(v) =
		Println("Client (Process-"+pid+") was served by Leader (Process-"+getLeader(procList)+") with consensus:"+v) >>
		clntServed := v
	-}
 

	{- Proposer Methods -}
	def generateNextSeqNo() = PropHighestSeqNo := PropHighestSeqNo? + numNodes
	
	def increment(x) = x := x? +1
	
	def sendPrepare() = setPropState(PHASE1) >>
						leader := pid >>
						Println("Process " + id + " in sendPrepare(). Thinks current leader is process " +leader?) >>
						generateNextSeqNo() >> message(PREP, PropHighestSeqNo?, 999) >m> broadcast(m) 
	
	def receivePromise(m, outch) = Println("Process " + id + " in receivePromise()") >> 
								   (if (m.getVal() = -1) then 
								    (increment(PropNumNotPromised) >>
								    Println("Process " + id + " received NACK in receivePromise() " + PropNumNotPromised?) >> 
								    	(if (PropNumNotPromised? >= PropQuorumSize?) then 
								    		(setPropState(RJT) >> sendPrepare()) 
								    	else signal))
								    else 
									   (increment(PropNumPromised) >> moveToAccept()))

	def moveToAccept() = if ((PropNumPromised? >= PropQuorumSize?) && (PropState? = PHASE1)) then 
							(setPropState(PHASE2) >> sendAccept()) 
						else 
							signal
	def sendAccept() = Println("Process " + id + " in sendAccept()") >>
					   message(ACPT, PropHighestSeqNo?, 999) >m> broadcast(m)
	
	def receiveAccepted(m, outch) = Println("Process " + id + " in receiveAccepted()") >> 
									(if (m.getVal() = -1) then 
										(increment(PropNumRejected) >>
											(if (PropNumRejected? >= PropQuorumSize?) then 
								    			setPropState(RJT) 
								    		else signal)) 
									else 
									   (increment(PropNumAccepted) >> moveToAccepted()))
									   
	def moveToAccepted() = if ((PropNumAccepted? >= PropQuorumSize?) && (PropState? = PHASE2)) then 
								(Println("Proposal Accepted for process "+pid+"'s proposed value ") >>
								setPropState(DCD))
						   else 
						   		signal
						   
						   
	{- Acceptor Methods -}
	
	def receivePrepare(m, outch) = Println("Process " + id + " in receivePrepare()") >>
	-- If Proposer and Seq no received is greater than PropHighestSeqNo, change role to Acceptor
								  (if (role? = PROP) then
								  		doPropReceivePrepare(m, outch)
								   else
								   		doAccpReceivePrepare(m, outch))
	
	def doPropReceivePrepare(m, outch) =
								if ((PropHighestSeqNo? <: m.getSeq())) then 
									(setRole(ACCP) >> Println("Changing " +pid+ " from PROP to ACCP" ) >>
									(if (AcpHighestSeqNo? <: m.getSeq()) then sendPromise(m, outch)  >>  
							     	AcpHighestSeqNo := m.getSeq() else sendPromiseNack(m, outch)))
							    else
							    	signal
							     
	def doAccpReceivePrepare(m, outch) = 
								 (if (AcpHighestSeqNo? <: m.getSeq()) then sendPromise(m, outch)  >>  
							     AcpHighestSeqNo := m.getSeq() else sendPromiseNack(m, outch))
							     
	def sendPromise(m, outch) = Println("Process " + id + " in sendPromise()") >> 
								outch.getId() > (src, dst) > leader := dst >>
								message(PRM, m.getSeq(), AcpHighestSeqNo?) >m> outch.put(m)
	
	def sendPromiseNack(m, outch) = Println("Process " + id + " in sendPromiseNack()") >>
									message(PRM, m.getSeq(), -1) >m> outch.put(m)
	
	def receiveAccept(m, outch) = Println("Process " + id + " in receiveAccept()") >>
								  (if (m.getSeq() >= AcpHighestSeqNo?) then sendAccepted(m, outch) >> sayListeners(m) 
									   >> AcpHighestSeqNo := m.getSeq() else sendAcceptedNack(m, outch))

	def sendAccepted(m, outch) = Println("Process " + id + " in sendAccept()") >>
								 Println("Process " + id + " thinks current leader is process " + leader?) >>
								 message(ACPTD, m.getSeq(), AcpHighestSeqNo?) >m> outch.put(m)

	def sendAcceptedNack(m, outch) = Println("Process " + id + " in sendAcceptNack()") >>
									 message(ACPTD, m.getSeq(), -1) >m> outch.put(m)
   
	def handlePropInMsg(m, outch) = (if (m.getType() = PRM) then receivePromise(m, outch) else
								    (if (m.getType() = ACPTD) then receiveAccepted(m, outch) else signal))
								  
	def handleAcpInMsg(m, outch) = (if (m.getType() = PREP) then receivePrepare(m, outch) else
								   (if (m.getType() = ACPT) then receiveAccept(m, outch) else signal))
								   
	def recvReplication(m) = 
		lsnrStore := m.getVal()
	
	def handleTimeout(src, dst) = --Println("Timeout Detected by Process " + dst + " for Process " + src + " leader = " + leader?) >> 
							 alive(src).write(false) >>
							 (if (src = leader?) then handleLeaderFailure() else signal)
							 
	def handleLeaderFailure() = --Println("Process " + pid + " in handleLeaderFailure() with maxAlive = " + maxAlive(numNodes-1)) >>
								(if (maxAlive(numNodes-1) = pid) then
									(Println("Process " + pid + " in handleLeaderFailure() is " + role? + " and is the maxAlive process. Making itself leader ") >>
									(if (role? = PROP) then (lock.acquire() >> sendPrepare() >> lock.release()) 
									 else (lock.acquire() >> setRole(PROP) >> 
									 		sendPrepare()) >> lock.release()))
								else signal)
	
	def maxAlive(0) = if (alive(0)? = true) then 0 else signal
	def maxAlive(i) = if (alive(i)? = true) then i else maxAlive(i-1)
								   
	def sendHBeat(ch) = 
		ch.getId() >(src, dst)> (message(HBT, -1, -1) >m> ch.put(m)) >> Rwait(timeout) >> sendHBeat(ch)

	def handleListInMsg(m, outch) = (if (m.getType() = REPN) then recvReplication(m) else signal)
	
	def handleInMsg("proposer", m, outch) = 
   		lock.acquire() >> handlePropInMsg(m, outch) >> handleAcpInMsg(m, outch) >> lock.release()
   		
	def handleInMsg("acceptor", m, outch) = 
		lock.acquire() >> handleAcpInMsg(m, outch) >> lock.release()
	
	def handleInMsg("listener", m, outch) = 
   		lock.acquire() >> handleListInMsg(m, outch) >> lock.release()

	
	def processMsg(m, ch) =  ch.getId() >(src,dst)> 
							(if (m.getType() = TMOUT) then
								handleTimeout(src, dst)
							else handleInMsg(role?, m, getOutch(ch, out:outs)))
							
	def handleMsg(ch) =  (processMsg(m, ch) <m< (ch.get() | (Rwait(3 * timeout) >> message(TMOUT, -1, -1)))) 
						 >> handleMsg(ch)
	
	def run() = (map(sendHBeat, out:outs) | map(handleMsg, in:ins))


	def printInfo() =
		Println("Process ID: "+pid + " Role: "+ role?) >> Println("In Channels: ") >> printChIds(in:ins) >>
		Println("Out Channels: ") >> printChIds(out:outs)>> Println("Listener channels ") >> 
		printChIds(lisnts)
 
stop


{- Class to encapsulate the whole system state: Processes and Channels.
   All processes are initialized here and the mapping to channels is stored.
   Roles of the processes can be changed later. Failures can be simulated by failing out-channels.
   CAUTION: There should be atleast 3 nodes in the system
            to prevent array bound errors
-}
def class distributedSystem(failProb, timeOut, numNodes, numListeners) =
	
	val lock = Semaphore(1)
	val chArray =  fillArray(Array(numNodes * numNodes), lambda(slot) = faultyChannel(failProb, (slot/numNodes, slot%numNodes))) -- array of all channels
	val procArray = Array(numNodes) 
  
	-- Initialize all channels in the Distributed System (UNUSED: see above declaration)
	def initChannels(i) = 
		if (i <: numNodes * numNodes) 
			then chArray(i) := faultyChannel(failProb, (i/numNodes, i%numNodes))  >>  initChannels(i+1)
		else signal
  
	-- Initialize all nodes in the Distributed System
	-- Should be called after initializing channels
	
	-- initialize the listeners/learners
	def initListeners(i) =
			if (i <: numListeners)
			then Println("initing listener-"+i) >>
				globalCurrent.write(i) >>
				filter(testChIn, arrayToList(chArray)) >ins>
				filter(testChOut1, arrayToList(chArray)) >out1>
				filter(testChListener, out1) >lisnts> 
				filter(testChOut2, arrayToList(chArray)) >outs>
				procArray(i) := process(i, timeOut, numNodes, LIST, ins, outs, lisnts) >>
				initListeners(i+1)
			else signal
	
	-- initialize acceptors, proposer and client
	def initProcs(i) =
		if (i <: numNodes)
			then Println("initing process-"+i) >>
				globalCurrent.write(i) >>
				filter(testChIn, arrayToList(chArray)) >ins>
				filter(testChOut1, arrayToList(chArray)) >out1> 
				filter(testChListener, out1) >lisnts> 
				filter(testChOut2, arrayToList(chArray)) >outs>
				(if (i = numListeners) then procArray(i) := process(i, timeOut, numNodes, ACCP, ins, outs, lisnts)
				 else
				   if (i = numListeners +1) then procArray(i) := process(i, timeOut, numNodes, ACCP, ins, outs, lisnts)
				   else procArray(i) := process(i, timeOut, numNodes, ACCP, ins, outs, lisnts)) >>
				 
				initProcs(i+1)
		else signal
		
	-- set the roles of listerner channels to differentiate them from regular channels
	def setListen(ch) = ch.setRole(LIST)
	def setChRoles(i) =
		if (i <: numListeners)
			then globalCurrent.write(i) >> 
				filter(testChIn, arrayToList(chArray)) >lisns> map(setListen, lisns) >>
				setChRoles(i+1)
			else signal
		
   -- Main function to initialize the system.
	def init() = setChRoles(0) >> initListeners(0)  >> initProcs(numListeners) >>
				Println("Initialzied "+numNodes+" processes.")
	
	
	-- get the list form of processes and channels
	def getProcList() = arrayToList(procArray)
	def getChList() = arrayToList(chArray)

	-- filter desired nodes from the whole bunch
	def getProposers()  = filter(testProposer, arrayToList(procArray))
	def getClients()    = filter(testClient, arrayToList(procArray))
	def getListeners()  = filter(testListener, arrayToList(procArray))
	def getAcceptors()  = filter(testAcceptor, arrayToList(procArray))
	
	-- redesignate a chosen node
	def setProposer(i) = procArray(i)?.setRole(PROP)
	def setAcceptor(i) = procArray(i)?.setRole(ACCP)

	
	-- print the information related to one/all processes
	def printProcessInfo(i) = procArray(i)?.printInfo()
	def printProcs() = 
		upto(numNodes) >i> lock.acquire()>> printProcessInfo(i) >> lock.release()
	def printProcesses([]) = signal
	def printProcesses(pr:prs) = pr.printInfo() >> printProcesses(prs)
	
	-- print information related to all channels
	def printChannels([]) = signal
	def printChannels(ch:chs) = ch.printInfo() >> printChannels(chs)
	
	
	-- methods to simulate failure and recovery of nodes/channels
	def setChFail(ch) = ch.setFail(100)
	def setChUnfail(ch) = ch.setFail(0)
	
	def failRandomNode() = 
		Random(numNodes) >i> map(setChFail, procArray(i)?.myOutChannels())
	def unfailRandomNode() = 
		Random(numNodes) >i> map(setChUnfail, procArray(i)?.myOutChannels())
	
	def failNode(i) =
		map(setChFail, procArray(i)?.myOutChannels())
	def unfailNode(i) =
		map(setChUnfail, procArray(i)?.myOutChannels())
	
stop

------------------------------------------- GOAL Expressions -------------------------------------

val sem = Semaphore(1) -- needed for proper printing of processes/channel information
val numListeners = 0 -- number of Listeners/Learner's in the system. Needed only if you want replication
val numNodes = 8 	-- total number of nodes in the distributed system
val failProb = 0 	-- probability of link/node failure
val timeOut = 1500 	-- timeout(ms) before declaring failure

-- Instantiate a distributed System
val ds = distributedSystem(failProb, timeOut, numNodes, numListeners)

-- Get the list of processes and channels
val procs = ds.getProcList()
val channels = ds.getChList()

-- Extract proposer(s) and Acceptors
val proposers = ds.getProposers()
val acceptors = ds.getAcceptors()

-- function to be run by all processes
def run(p) = p.run()

-- funtion to be run by the initial proposer to trigger the protocol
def start(p) = p.sendPrepare()

def printId(p) = Print("Process: "+p.getId())

-- print the list of acceptors and proposers' information
def printAcceptors() = sem.acquire() >> ds.getAcceptors() >a> ds.printProcesses(a) >> sem.release()
def printProposers() = sem.acquire() >> ds.getProposers() >p> ds.printProcesses(p) >> sem.release()

-- Randomly fail and unfail nodes. Doesn't guarantee convergence. Correctness is guaranteed, not liveness.
def failUnfailRandom() = 
	Random(4000) >r> Rwait(r) >> ds.failRandomNode() >> Rwait(4000) >> ds.unfailRandomNode() >> 
	Rwait(4000) >> failUnfailRandom()

Println("============= Starting Classic Paxos Simulation =========") >> ds.init() >>
Println("======== DS Initialized ===========") >>
--Println("Acceptor(s) are: ") >>
--printAcceptors() >>
--Println("Proposer(s) are: ") >>
--printProposers() >> 

-- Starts the simulation with/without convergence due to random failures and recoveries
-- (map(start, ds.getProposers()) >> map(run, ds.getProcList())) | failUnfailRandom()

{- Starts the simulation with convergence
  - The simulation runs forever (HeartBeat messages will be sent) and will need to be stopped
    when a proposal gets accepted for last value.
  - 
-}
(map(start, ds.getProposers()) >> map(run, ds.getProcList())) | 
(Rwait(6000) >> (ds.failNode(0)) >> Rwait(3000) >> ds.failNode(7)) 
>> Rwait(8000) >> ds.failNode(6) >> ds.unfailNode(7) >> Rwait(8000) >> ds.failNode(5)
