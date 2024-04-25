/**
 *
 */
package no.hvl.dat110.middleware;

import java.math.BigInteger;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.LamportClock;
import no.hvl.dat110.util.Util;

/**
 * @author tdoy
 *
 */
public class MutualExclusion {

	private static final Logger logger = LogManager.getLogger(MutualExclusion.class);
	/** lock variables */
	private boolean CS_BUSY = false; // indicate to be in critical section (accessing a shared resource)
	private boolean WANTS_TO_ENTER_CS = false; // indicate to want to enter CS
	private List<Message> queueack; // queue for acknowledged messages
	private List<Message> mutexqueue; // queue for storing process that are denied permission. We really don't need
	// this for quorum-protocol

	private LamportClock clock; // lamport clock
	private Node node;

	public MutualExclusion(Node node) throws RemoteException {
		this.node = node;

		clock = new LamportClock();
		queueack = new ArrayList<Message>();
		mutexqueue = new ArrayList<Message>();
	}

	public void acquireLock() {
		CS_BUSY = true;
	}

	public void releaseLocks() {
		WANTS_TO_ENTER_CS = false;
		CS_BUSY = false;
	}

	public boolean doMutexRequest(Message message, byte[] updates) throws RemoteException {

		logger.info(node.nodename + " wants to access CS");
		// clear the queueack before requesting for votes

		// clear the mutexqueue

		// increment clock

		// adjust the clock on the message, by calling the setClock on the message

		// wants to access resource - set the appropriate lock variable

		// start MutualExclusion algorithm

		// first, call removeDuplicatePeersBeforeVoting. A peer can hold/contain 2
		// replicas of a file. This peer will appear twice

		// multicast the message to activenodes (hint: use multicastMessage)

		// check that all replicas have replied (permission) -
		// areAllMessagesReturned(int numvoters)?

		// if yes, acquireLock

		// send the updates to all replicas by calling node.broadcastUpdatetoPeers

		// clear the mutexqueue
		// Clear the queueack before requesting for votes
		queueack.clear();

		// Clear the mutexqueue
		mutexqueue.clear();

		// Increment clock
		clock.increment();


		// Adjust the clock on the message
		message.setClock(clock.getClock());

		// Wants to access resource - set the appropriate lock variable
		WANTS_TO_ENTER_CS = true;

		// Start MutualExclusion algorithm

		// First, call removeDuplicatePeersBeforeVoting
		List <Message> msgList = this.removeDuplicatePeersBeforeVoting();

		//List<Message> uniquePeers = new ArrayList<>(node.activenodesforfile);

		// Multicast the message to activenodes
		multicastMessage(message, msgList);

		// check that all replicas have replied (permission)
		boolean permission = areAllMessagesReturned(msgList.size());

		if (permission) {
			acquireLock();
			// node.broadcastUpdatetoPeers
			node.broadcastUpdatetoPeers(updates);
			// clear the mutexqueue
			mutexqueue.clear();
			//return permission
			return permission;
		}
		return false;
	}
		/*if (areAllMessagesReturned(msgList.size())) {
			// AcquireLock
			acquireLock();

			// Send the updates to all replicas
			node.broadcastUpdatetoPeers(updates);

			// Clear the mutexqueue
			mutexqueue.clear();

			// Return permission
			//return true;
		}

		return areAllMessagesReturned(uniquePeers.size());
	}*/

	// multicast message to other processes including self
	private void multicastMessage(Message message, List<Message> activenodes) throws RemoteException {

		logger.info("Number of peers to vote = " + activenodes.size());

		activenodes.forEach(n -> {
			//for (Message activeNode : activenodes) {
			try {
				if (n.getNodeName().equals(node.nodename)) {
					this.onMutexRequestReceived(message);
				} else {
					NodeInterface stub = Util.getProcessStub(n.getNodeName(), n.getPort());

					stub.onMutexRequestReceived(message);

				}
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		});
	}
		// iterate over the activenodes

		// obtain a stub for each node from the registry

		// call onMutexRequestReceived()



	public void onMutexRequestReceived(Message message) throws RemoteException {

		// increment the local clock

		// if message is from self, acknowledge, and call
		// onMutexAcknowledgementReceived()
		clock.increment();
		if (message.getNodeName().equals(node.getNodeName())) {
			node.onMutexAcknowledgementReceived(message);
			message.setAcknowledged(true);
			return;
		}
		int caseid = -1;
		if (!CS_BUSY && !WANTS_TO_ENTER_CS)
			caseid = 0;
		else if (CS_BUSY)
			caseid = 1;
		else if (!CS_BUSY && WANTS_TO_ENTER_CS) // && (message.getClock() < clock.getClock()  || (message.getClock() == clock.getClock() && message.getNodeID().compareTo(node.getNodeID()) < 0)))
			caseid = 2;

		/*
		 * write if statement to transition to the correct caseid in the
		 * doDecisionAlgorithm
		 */

		// caseid=0: Receiver is not accessing shared resource and does not want to
		// (send OK to sender)

		// caseid=1: Receiver already has access to the resource (dont reply but queue
		// the request)

		// caseid=2: Receiver wants to access resource but is yet to - compare own
		// message clock to received message's clock

		// check for decision
		doDecisionAlgorithm(message, mutexqueue, caseid);
	}


	public void doDecisionAlgorithm(Message message, List<Message> queue, int condition) throws RemoteException {

		String procName = message.getNodeName();
		int port = message.getPort();

		switch (condition) {


			/** case 1: Receiver is not accessing shared resource and does not want to (send
			 * OK to sender)
			 **/


			case 0: {
				NodeInterface stub = Util.getProcessStub(procName, port);
				message.setAcknowledged(true);
				stub.onMutexAcknowledgementReceived(message);

				// get a stub for the sender from the registry

				// acknowledge message

				// send acknowledgement back by calling onMutexAcknowledgementReceived()

				break;
			}

			/**
			 * case 2: Receiver already has access to the resource (dont reply but queue the
			 * request)
			 **/


			case 1: {
				queue.add(message);
				// queue this message
				break;
			}

			/** case 3: Receiver wants to access resource but is yet to (compare own message
			 * clock to received message's clock the message with lower timestamp wins) -
			 * send OK if received is lower. Queue message if received is higher
			 **/

			case 2: {
				int sndclock = message.getClock();

				int ownclock = node.getMessage().getClock();
				BigInteger sndprocid = message.getNodeID();

				boolean allow = false;

				if (sndclock < ownclock) {
					allow = true;
				} else if (sndclock == ownclock && sndprocid.compareTo(node.getNodeID()) == -1) {
					allow = true;

				} else {
					queue.add(message);
				}
				if (allow) {
					NodeInterface stub = Util.getProcessStub(procName, port);
					message.setAcknowledged(true);
					stub.onMutexAcknowledgementReceived(message);
				}

			}
			break;
			default:
				break;
		}
	}

		/*	case 2: {
				// Check if the sender's clock is lower or if it's the same but the sender's node ID is lower
				if (message.getClock() < node.getMessage().getClock() || (message.getClock() == node.getMessage().getClock() && message.getNodeID().compareTo(node.getNodeID()) < 0)) {
					// If the sender has priority, acknowledge the message and send OK to the sender
					queueack.add(message);
					message.setAcknowledged(true);
					node.onMutexAcknowledgementReceived(message);
				} else {
					// If the receiver has priority, queue the request
					mutexqueue.add(message);
				}
				break;
			}


		default:
				break;
		}

	}*/


            public void onMutexAcknowledgementReceived (Message message) throws RemoteException {

                // add message to queueack
                queueack.add(message);


            }

            // multicast release locks message to other processes including self
            public void multicastReleaseLocks (Set < Message > activenodes) throws RemoteException {
                logger.info("Releasing locks from = " + activenodes.size());

		activenodes.forEach(n -> {
			//for (Message activeNode : activenodes) {
			try {
			if (n.getNodeName().equals(node.nodename)) {
				this.releaseLocks();
			} else {
				NodeInterface stub = Util.getProcessStub(n.getNodeName(), n.getPort());

					stub.releaseLocks();
				}
				}catch (RemoteException e) {
				throw new RuntimeException(e);
			}

		});
		}


               /* for (Message activeNode : activenodes) {
                    try {
                        if (activeNode.getNodeName().equals(node.nodename)) {
                            this.releaseLocks();
                        } else {
                            NodeInterface stub = Util.getProcessStub(activeNode.getNodeName(), activeNode.getPort());

                            stub.releaseLocks();
                        }
                    } catch (RemoteException e) {
                        e.printStackTrace();
                    }
                }
                ;
            }*/
            // iterate over the activenodes

            // obtain a stub for each node from the registry

            // call releaseLocks()


            private boolean areAllMessagesReturned ( int numvoters) throws RemoteException {
				logger.info(node.getNodeName() + ": size of queueack = " + queueack.size());

				// check if the size of the queueack is the same as the numvoters

				// clear the queueack

				// return true if yes and false if no
				if (queueack.size() == numvoters) {
					queueack.clear();
					return true;
				} else {

					return false;
				}
			}

            private List<Message> removeDuplicatePeersBeforeVoting () {

                List<Message> uniquepeer = new ArrayList<Message>();
                for (Message p : node.activenodesforfile) {
                    boolean found = false;
                    for (Message p1 : uniquepeer) {
                        if (p.getNodeName().equals(p1.getNodeName())) {
                            found = true;
                            break;
                        }
                    }
                    if (!found)
                        uniquepeer.add(p);
                }
                return uniquepeer;
            }
        }