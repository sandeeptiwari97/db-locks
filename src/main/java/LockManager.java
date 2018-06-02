import com.sun.org.apache.regexp.internal.RE;

import java.util.*;

/**
 * The Lock Manager handles lock and unlock requests from transactions. The
 * Lock Manager will maintain a hash table that is keyed on the resource
 * being locked. The Lock Manager will also keep a FIFO queue of requests
 * for locks that cannot be immediately granted.
 */
public class LockManager {

    public enum LockType {
        S,
        X,
        IS,
        IX
    }

    private HashMap<Resource, ResourceLock> resourceToLock;

    public LockManager() {
        this.resourceToLock = new HashMap<Resource, ResourceLock>();

    }

    /**
     * The acquire method will grant the lock if it is compatible. If the lock
     * is not compatible, then the request will be placed on the requesters
     * queue.
     * @param transaction that is requesting the lock
     * @param resource that the transaction wants
     * @param lockType of requested lock
     */
    public void acquire(Transaction transaction, Resource resource, LockType lockType)
            throws IllegalArgumentException {
        // HW5: To do
        if (transaction.getStatus() == Transaction.Status.Waiting) {
            throw new IllegalArgumentException();
        }

        if (resource.getResourceType() == Resource.ResourceType.PAGE) {
            if ((lockType.equals(LockType.IX)) || (lockType.equals(LockType.IS))) {
                throw new IllegalArgumentException();
            }
            if (resourceToLock.size() == 0) {
                throw new IllegalArgumentException();
            }
            for (Resource r : resourceToLock.keySet()) {
                if ((r.getResourceType() == Resource.ResourceType.TABLE)
                        && (r.getTableName() == resource.getTableName())) {
                    ResourceLock rLock = resourceToLock.get(r);
                    if (lockType.equals(LockType.S)) {
                        if (!(rLock.lockOwners.contains(new Request(transaction, LockType.IX))
                                || (rLock.lockOwners.contains(new Request(transaction, LockType.IS))))) {
                            throw new IllegalArgumentException();
                        }
                    }
                    if (lockType.equals(LockType.X)) {
                        if (!(rLock.lockOwners.contains(new Request(transaction, LockType.IX)))) {
                            throw new IllegalArgumentException();
                        }
                    }
                }
            }
        }

        Request request = new Request(transaction, lockType);
        ResourceLock lock = resourceToLock.get(resource);

        if (lock == null) {
            lock = new ResourceLock();
        }

        if (holds(transaction, resource, LockType.S)) {
            if (lockType.equals(LockType.X)) {
                Request removeThis = new Request(transaction, LockType.S);
                lock.lockOwners.remove(removeThis);
                resourceToLock.put(resource, lock);
                if (compatible(resource, transaction, lockType)) {
                    lock.lockOwners.add(request);
                    resourceToLock.put(resource, lock);
                    return;
                } else {
                    lock.lockOwners.add(removeThis);
                    lock.requestersQueue.addFirst(request);
//                    transaction.sleep();
                    resourceToLock.put(resource, lock);
                    return;
                }
            }
        }

        if (compatible(resource, transaction, lockType)) {
            lock.lockOwners.add(request);
            resourceToLock.put(resource, lock);
        } else {
//            if (holds(transaction, resource, LockType.S)) {
//                if (lockType.equals(LockType.X)) {
//                    Request removeThis = new Request(transaction, LockType.S);
//                    lock.lockOwners.remove(removeThis);
//                    resourceToLock.put(resource, lock);
//                    if (compatible(resource, transaction, LockType.X)) {
//                        lock.lockOwners.add(request);
//                        resourceToLock.put(resource, lock);
//                        return;
//                    } else {
//                        lock.lockOwners.add(removeThis);
//                        lock.requestersQueue.addFirst(request);
////                        transaction.sleep();
//                        resourceToLock.put(resource, lock);
//                        return;
//                    }
//                }
//            }
            lock.requestersQueue.add(request);
            transaction.sleep();
            resourceToLock.put(resource, lock);
        }
    }

    /**
     * Checks whether the a transaction is compatible to get the desired lock on the given resource
     * @param resource the resource we are looking it
     * @param transaction the transaction requesting a lock
     * @param lockType the type of lock the transaction is request
     * @return true if the transaction can get the lock, false if it has to wait
     */
    private boolean compatible(Resource resource, Transaction transaction, LockType lockType) {
        // HW5: To do
        ResourceLock lock = resourceToLock.get(resource);
        if (lock == null) {
            return true;
        }

        for (Request r : lock.lockOwners) {
            if (r.transaction.equals(transaction)) {
                if (r.lockType.equals(lockType)) {
                    throw new IllegalArgumentException();
                } else {
                    if (r.lockType.equals(LockType.X)) {
                        if (lockType.equals(LockType.S)) {
                            throw new IllegalArgumentException();
                        }
                    }
                    if (r.lockType.equals(LockType.IX)) {
                        if (lockType.equals(LockType.IS)) {
                            throw new IllegalArgumentException();
                        }
                    }
                    if (resource.getResourceType() == Resource.ResourceType.PAGE) {
                        if ((lockType.equals(LockType.IS)) || (lockType.equals(LockType.IX))) {
                            throw new IllegalArgumentException();
                        }
                    }
                }
            } else {
                if ((lockType.equals(LockType.X)) || (r.lockType.equals(LockType.X))) {
                    return false;
                }
                if (r.lockType.equals(LockType.IX)) {
                    if (lockType.equals(LockType.S)) {
                        return false;
                    }
                }
                if (r.lockType.equals(LockType.S)) {
                    if (lockType.equals(LockType.IX)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * Will release the lock and grant all mutually compatible transactions at
     * the head of the FIFO queue. See spec for more details.
     * @param transaction releasing lock
     * @param resource of Resource being released
     */
    public void release(Transaction transaction, Resource resource) throws IllegalArgumentException{
        // HW5: To do
        if (transaction.getStatus() == Transaction.Status.Waiting) {
            throw new IllegalArgumentException();
        }
        ResourceLock lock = resourceToLock.get(resource);

        if (lock == null) {
            throw new IllegalArgumentException();
        }


        for (int i = 0; i < lock.lockOwners.size(); i++) {
            if (lock.lockOwners.get(i).transaction.equals(transaction)) {
                if (resource.getResourceType() == Resource.ResourceType.PAGE) {
                    resourceToLock.get(resource).lockOwners.remove(i);
                } else {
                    for (Page page : ((Table) resource).getPages()) {
                        for (Request r : resourceToLock.get(page).lockOwners) {
                            if (transaction.equals(r.transaction)) {
                                throw new IllegalArgumentException();
                            }
                        }
                    }
                    resourceToLock.get(resource).lockOwners.remove(i);
                }
                promote(resource);
            }
        }
        
        ArrayList<Request> rmv = new ArrayList<>();
        for (Request r : lock.lockOwners) {
            if (r.transaction.equals(transaction)) {
                rmv.add(r);
            }
        }
        lock.lockOwners.removeAll(rmv);
        promote(resource);
    }

    /**
     * This method will grant mutually compatible lock requests for the resource
     * from the FIFO queue.
     * @param resource of locked Resource
     */
     private void promote(Resource resource) {
         // HW5: To do
         ResourceLock lock = resourceToLock.get(resource);
         if (lock == null) {
             throw new IllegalArgumentException();
         }
         LinkedList<Request> rq = lock.requestersQueue;
         if (rq.size() > 0) {
             boolean compatible = compatible(resource, rq.peek().transaction, rq.peek().lockType);
             while (compatible) {
                 Request request = rq.poll();
                 if (request != null) {
                     lock.lockOwners.add(request);
                     request.transaction.wake();
                 }
                 if (rq.peek() == null || !compatible(resource, rq.peek().transaction, rq.peek().lockType)) {
                     compatible = false;
                 }
             }
         }
         return;
     }

    /**
     * Will return true if the specified transaction holds a lock of type
     * lockType on the resource.
     * @param transaction potentially holding lock
     * @param resource on which we are checking if the transaction has a lock
     * @param lockType of lock
     * @return true if the transaction holds lock
     */
    public boolean holds(Transaction transaction, Resource resource, LockType lockType) {
        // HW5: To do
        ResourceLock lock = resourceToLock.get(resource);
        if (lock == null) {
            return false;
        }
        for (int i = 0; i < lock.lockOwners.size(); i++) {
            if (lock.lockOwners.get(i).transaction.equals(transaction)
                    && (lock.lockOwners.get(i).lockType.equals(lockType))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Contains all information about the lock for a specific resource. This
     * information includes lock owner(s), and lock requester(s).
     */
    private class ResourceLock {
        private ArrayList<Request> lockOwners;
        private LinkedList<Request> requestersQueue;

        public ResourceLock() {
            this.lockOwners = new ArrayList<Request>();
            this.requestersQueue = new LinkedList<Request>();
        }

    }

    /**
     * Used to create request objects containing the transaction and lock type.
     * These objects will be added to the requester queue for a specific resource
     * lock.
     */
    private class Request {
        private Transaction transaction;
        private LockType lockType;

        public Request(Transaction transaction, LockType lockType) {
            this.transaction = transaction;
            this.lockType = lockType;
        }

        @Override
        public String toString() {
            return String.format(
                    "Request(transaction=%s, lockType=%s)",
                    transaction, lockType);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            } else if (o instanceof Request) {
                Request otherRequest  = (Request) o;
                return otherRequest.transaction.equals(this.transaction) && otherRequest.lockType.equals(this.lockType);
            } else {
                return false;
            }
        }
    }
}
