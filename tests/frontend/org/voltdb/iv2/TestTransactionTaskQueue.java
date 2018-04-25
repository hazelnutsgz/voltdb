/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.iv2;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import org.junit.Test;
import org.voltdb.StarvationTracker;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

import junit.framework.TestCase;

public class TestTransactionTaskQueue extends TestCase
{
    private static final int SITE_COUNT = 2;
    private List<TransactionTaskQueue> m_txnTaskQueues = new ArrayList<>();
    private List<SiteTaskerQueue> m_siteTaskQueues = new ArrayList<>();
    List<Deque<TransactionTask>> m_expectedOrders = new ArrayList<>();
    long[] m_localTxnId = new long[SITE_COUNT]; // for sp txn, txnId is spHandle
    long m_mpTxnId = 0;

    private static SiteTaskerQueue getSiteTaskerQueue() {
        SiteTaskerQueue queue = new SiteTaskerQueue(0);
        queue.setStarvationTracker(new StarvationTracker(0));
        queue.setupQueueDepthTracker(0);
        return queue;
    }

    // Cases to test:
    // several single part txns

    private SpProcedureTask createSpProc(long localTxnId,
                                         TransactionTaskQueue queue)
    {
        // Mock an initiate message; override its txnid to return
        // the default SP value (usually set by ClientInterface).
        Iv2InitiateTaskMessage init = mock(Iv2InitiateTaskMessage.class);
        when(init.getTxnId()).thenReturn(Iv2InitiateTaskMessage.UNUSED_MP_TXNID);
        when(init.getSpHandle()).thenReturn(localTxnId);

        InitiatorMailbox mbox = mock(InitiatorMailbox.class);
        when(mbox.getHSId()).thenReturn(1337l);

        SpProcedureTask task =
            new SpProcedureTask(mbox, "TestProc", queue, init);
        return task;
    }

    private FragmentTask createFrag(long localTxnId, long mpTxnId,
            TransactionTaskQueue queue) {
        return createFrag(localTxnId, mpTxnId, queue, false);
    }
    // Create the first fragment of a MP txn
    private FragmentTask createFrag(long localTxnId, long mpTxnId,
                                    TransactionTaskQueue queue,
                                    boolean forReplay)
    {
        FragmentTaskMessage msg = mock(FragmentTaskMessage.class);
        when(msg.getTxnId()).thenReturn(mpTxnId);
        when(msg.isForReplay()).thenReturn(forReplay);
        InitiatorMailbox mbox = mock(InitiatorMailbox.class);
        when(mbox.getHSId()).thenReturn(1337l);
        ParticipantTransactionState pft =
            new ParticipantTransactionState(localTxnId, msg);
        FragmentTask task =
            new FragmentTask(mbox, pft, queue, msg, null);
        return task;
    }

    // Create follow-on fragments of an MP txn
    private FragmentTask createFrag(TransactionState txn, long mpTxnId,
                                    TransactionTaskQueue queue)
    {
        FragmentTaskMessage msg = mock(FragmentTaskMessage.class);
        when(msg.getTxnId()).thenReturn(mpTxnId);
        InitiatorMailbox mbox = mock(InitiatorMailbox.class);
        when(mbox.getHSId()).thenReturn(1337l);
        FragmentTask task =
            new FragmentTask(mbox, (ParticipantTransactionState)txn, queue, msg, null);
        return task;
    }

    private CompleteTransactionTask createComplete(TransactionState txn,
            long mpTxnId,
            TransactionTaskQueue queue)
    {
        CompleteTransactionMessage msg = mock(CompleteTransactionMessage.class);
        when(msg.getTxnId()).thenReturn(mpTxnId);
        when(msg.getTimestamp()).thenReturn(CompleteTransactionMessage.INITIAL_TIMESTAMP);
        CompleteTransactionTask task =
                new CompleteTransactionTask(mock(InitiatorMailbox.class), txn, queue, msg);
        return task;
    }
    private CompleteTransactionTask createRepairComplete(TransactionState txn,
                                                       long mpTxnId,
                                                       TransactionTaskQueue queue)
    {
        CompleteTransactionMessage msg = mock(CompleteTransactionMessage.class);
        when(msg.getTxnId()).thenReturn(mpTxnId);
        MpRestartSequenceGenerator generator = new MpRestartSequenceGenerator(0, false);
        long seq = generator.getNextSeqNum();
        when(msg.getTimestamp()).thenReturn(seq);
        CompleteTransactionTask task =
            new CompleteTransactionTask(mock(InitiatorMailbox.class), txn, queue, msg);
        return task;
    }

    private CompleteTransactionTask createRestartComplete(TransactionState txn,
                                                         long mpTxnId,
                                                         TransactionTaskQueue queue)
    {
        CompleteTransactionMessage msg = mock(CompleteTransactionMessage.class);
        when(msg.getTxnId()).thenReturn(mpTxnId);
        MpRestartSequenceGenerator generator = new MpRestartSequenceGenerator(0, true);
        long seq = generator.getNextSeqNum();
        when(msg.getTimestamp()).thenReturn(seq);
        CompleteTransactionTask task =
                new CompleteTransactionTask(mock(InitiatorMailbox.class), txn, queue, msg);
        return task;
    }

    private void addTask(TransactionTask task, TransactionTaskQueue dut,
                         Deque<TransactionTask> teststorage)
    {
        if (teststorage != null) {
            teststorage.addLast(task);
        }
        dut.offer(task);
        dut.flush(task.getTxnId());
    }

    private void addMissingCompletionTask(TransactionTask task, TransactionTaskQueue dut) {
        dut.handleCompletionForMissingTxn((CompleteTransactionTask) task);
        dut.flush(task.getTxnId());
    }

    private void verify() throws InterruptedException {
        for (int i = 0; i < SITE_COUNT; i++) {
            assertEquals(m_expectedOrders.get(i).size(), m_siteTaskQueues.get(i).size());
            while (!m_expectedOrders.get(i).isEmpty()) {
                TransactionTask next_poll = (TransactionTask)m_siteTaskQueues.get(i).take();
                TransactionTask expected = m_expectedOrders.get(i).removeFirst();
                assertEquals(expected.getSpHandle(), next_poll.getSpHandle());
                assertEquals(expected.getTxnId(), next_poll.getTxnId());
            }
        }
    }

    @Override
    public void setUp() {
        for (int i = 0; i < SITE_COUNT; i++) {
            SiteTaskerQueue siteTaskQueue = getSiteTaskerQueue();
            m_siteTaskQueues.add(siteTaskQueue);
            TransactionTaskQueue txnTaskQueue = new TransactionTaskQueue(siteTaskQueue, SITE_COUNT);
            txnTaskQueue.initializeScoreboard(i);
            m_txnTaskQueues.add(txnTaskQueue);
            Deque<TransactionTask> expectedOrder = new ArrayDeque<>();
            m_expectedOrders.add(expectedOrder);
            m_localTxnId[0] = 0;
        }
    }

    @Override
    public void tearDown() {
        m_siteTaskQueues.clear();
        m_txnTaskQueues.clear();
        m_expectedOrders.clear();
        for (int i = 0; i < SITE_COUNT; i++) {
            m_localTxnId[i] = 0;
        }
        m_mpTxnId = 0;
        TransactionTaskQueue.resetScoreboards();
    }

    // This is the most common case
    @Test
    public void testBasicMultiFragmentsMp() throws InterruptedException {
        // Every site receives first fragment
        long txnId = m_mpTxnId++;
        TransactionTask firstFrag = null;
        for (int i = 0; i < SITE_COUNT; i++) {
            firstFrag = createFrag(m_localTxnId[i]++, txnId, m_txnTaskQueues.get(i));
            addTask(firstFrag, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        // Every site receives second fragment
        for (int i = 0; i < SITE_COUNT; i++) {
            TransactionTask next = createFrag(firstFrag.getTransactionState(), txnId, m_txnTaskQueues.get(i));
            addTask(next, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        // Every site receives a completion
        TransactionTask comp = null;
        for (int i = 0; i < SITE_COUNT; i++) {
            comp = createComplete(firstFrag.getTransactionState(), firstFrag.getTxnId(), m_txnTaskQueues.get(i));
            addTask(comp, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        verify();
    }

    // In case MpProc doesn't generate any fragment, e.g. run() method is empty
    @Test
    public void testNoFragmentMp() throws InterruptedException {
        long txnId = m_mpTxnId++;
        // Every site receives a completion
        TransactionTask comp = null;
        for (int i = 0; i < SITE_COUNT; i++) {
            MpTransactionState txnState = mock(MpTransactionState.class);
            comp = createComplete(txnState, txnId, m_txnTaskQueues.get(i));
            addTask(comp, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        verify();
    }

    // MpProc is in progress, a node failure cause MPI to repair previous transaction and restart current transaction
    @Test
    public void testMpRepair() throws InterruptedException {
        // Every site receives first fragment
        long txnId = m_mpTxnId++;
        TransactionTask[] firstFrag = new TransactionTask[SITE_COUNT];
        for (int i = 0; i < SITE_COUNT; i++) {
            firstFrag[i] = createFrag(m_localTxnId[i]++, txnId, m_txnTaskQueues.get(i));
            addTask(firstFrag[i], m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        // Not all sites receive completion
        TransactionTask comp = null;
        for (int i = 0; i < SITE_COUNT - 1; i++) {
            comp = createComplete(firstFrag[i].getTransactionState(), firstFrag[i].getTxnId(), m_txnTaskQueues.get(i));
            addTask(comp, m_txnTaskQueues.get(i), new ArrayDeque<TransactionTask>());
        }

        txnId = m_mpTxnId++;
        // it will stay at backlog
        TransactionTask firstFragOfNextTxn = null;
        for (int i = 0; i < SITE_COUNT - 1; i++) {
            firstFragOfNextTxn = createFrag(m_localTxnId[i]++, txnId, m_txnTaskQueues.get(i));
            addTask(firstFragOfNextTxn, m_txnTaskQueues.get(i), new ArrayDeque<TransactionTask>());
        }

        // failure occurs, MPI checks repair logs from everybody, decide to repair previous transaction,
        // restart current transaction

        // MPI sends repair completion
        for (int i = 0; i < SITE_COUNT; i++) {
            comp = createRepairComplete(firstFrag[i].getTransactionState(), firstFrag[i].getTxnId(), m_txnTaskQueues.get(i));
            // Finish the transaction
            firstFrag[i].getTransactionState().setDone();
            addTask(comp, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        // MPI sends restart completion
        for (int i = 0; i < SITE_COUNT; i++) {
            comp = createRestartComplete(firstFragOfNextTxn.getTransactionState(), firstFragOfNextTxn.getTxnId(), m_txnTaskQueues.get(i));
            addTask(comp, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        // Restart the framgments
        TransactionTask restartFrag = null;
        for (int i = 0; i < SITE_COUNT; i++) {
            restartFrag = createFrag(m_localTxnId[i]++, firstFragOfNextTxn.getTxnId(), m_txnTaskQueues.get(i));
            addTask(restartFrag, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        for (int i = 0; i < SITE_COUNT; i++) {
            comp = createComplete(restartFrag.getTransactionState(), restartFrag.getTxnId(), m_txnTaskQueues.get(i));
            addTask(comp, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        verify();
    }

    // Sometimes, especially in MPI failover, site may gets staled completion because of slow network.
    @Test
    public void testStaledCompletion() throws InterruptedException {
        // Every site receives first fragment
        long txnId = m_mpTxnId++;
        TransactionTask[] firstFrag = new TransactionTask[SITE_COUNT];
        for (int i = 0; i < SITE_COUNT; i++) {
            firstFrag[i] = createFrag(m_localTxnId[i]++, txnId, m_txnTaskQueues.get(i));
            addTask(firstFrag[i], m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        // Not all sites receive completion
        TransactionTask comp = null;
        for (int i = 0; i < SITE_COUNT - 1; i++) {
            comp = createComplete(firstFrag[i].getTransactionState(), firstFrag[i].getTxnId(), m_txnTaskQueues.get(i));
            addTask(comp, m_txnTaskQueues.get(i), new ArrayDeque<TransactionTask>());
        }

        // failure occurs, MPI repair current transaction

        // Every site gets the repair completion message
        for (int i = 0; i < SITE_COUNT; i++) {
            comp = createRestartComplete(firstFrag[i].getTransactionState(), firstFrag[i].getTxnId(), m_txnTaskQueues.get(i));
            // Finish the transaction
            firstFrag[i].getTransactionState().setDone();
            addTask(comp, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        // But on one site, a staled completion arrives, it should be discarded.
        for (int i = SITE_COUNT - 1; i < SITE_COUNT; i++) {
            comp = createComplete(firstFrag[i].getTransactionState(), firstFrag[i].getTxnId(), m_txnTaskQueues.get(i));
            addTask(comp, m_txnTaskQueues.get(i), new ArrayDeque<TransactionTask>());
        }

        verify();
    }

    // MPI may send repair messages multiple times because every SPI promotion interrupts repair.
    @Test
    public void testMultipleFailures() throws InterruptedException {
        // Every site receives first fragment
        long txnId = m_mpTxnId++;
        TransactionTask[] firstFrag = new TransactionTask[SITE_COUNT];
        for (int i = 0; i < SITE_COUNT; i++) {
            firstFrag[i] = createFrag(m_localTxnId[i]++, txnId, m_txnTaskQueues.get(i));
            addTask(firstFrag[i], m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        // Every site receives completion
        TransactionTask comp = null;
        for (int i = 0; i < SITE_COUNT; i++) {
            comp = createComplete(firstFrag[i].getTransactionState(), firstFrag[i].getTxnId(), m_txnTaskQueues.get(i));
            // Finish the transaction
            firstFrag[i].getTransactionState().setDone();
            addTask(comp, m_txnTaskQueues.get(i), new ArrayDeque<TransactionTask>());
        }

        // Run next mp transaction
        txnId = m_mpTxnId++;
        TransactionTask[] firstFragOfNextTxn = new TransactionTask[SITE_COUNT];
        for (int i = 0; i < SITE_COUNT; i++) {
            firstFragOfNextTxn[i] = createFrag(m_localTxnId[i]++, txnId, m_txnTaskQueues.get(i));
            addTask(firstFragOfNextTxn[i], m_txnTaskQueues.get(i), new ArrayDeque<TransactionTask>());
        }

        // failure occurs, MPI repair current transaction

        // Every site gets the repair completion and restart completion messages
        for (int i = 0; i < SITE_COUNT; i++) {
            comp = createRestartComplete(firstFrag[i].getTransactionState(), firstFrag[i].getTxnId(), m_txnTaskQueues.get(i));
            addTask(comp, m_txnTaskQueues.get(i), m_expectedOrders.get(i));

            TransactionTask restartFrag = createFrag(m_localTxnId[i]++, firstFragOfNextTxn[i].getTxnId(), m_txnTaskQueues.get(i));
            addTask(restartFrag, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        // Finish the transaction, flush the backlog
        for (int i = 0; i < SITE_COUNT; i++) {
            firstFrag[i].getTransactionState().setDone();
            m_txnTaskQueues.get(i).flush(firstFrag[i].getTxnId());
        }

        // failure occurs, MPI repair current transaction

        // Every site gets the repair completion and restart completion messages
        for (int i = 0; i < SITE_COUNT; i++) {
            comp = createRestartComplete(firstFrag[i].getTransactionState(), firstFrag[i].getTxnId(), m_txnTaskQueues.get(i));
            addTask(comp, m_txnTaskQueues.get(i), m_expectedOrders.get(i));

            TransactionTask restartFrag = createFrag(m_localTxnId[i]++, firstFragOfNextTxn[i].getTxnId(), m_txnTaskQueues.get(i));
            addTask(restartFrag, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        // Finish the transaction, flush the backlog
        for (int i = 0; i < SITE_COUNT; i++) {
            firstFrag[i].getTransactionState().setDone();
            m_txnTaskQueues.get(i).flush(firstFrag[i].getTxnId());
        }

        // failure occurs, MPI repair current transaction

        // Every site gets the repair completion and restart completion messages
        for (int i = 0; i < SITE_COUNT; i++) {
            comp = createRestartComplete(firstFrag[i].getTransactionState(), firstFrag[i].getTxnId(), m_txnTaskQueues.get(i));
            addTask(comp, m_txnTaskQueues.get(i), m_expectedOrders.get(i));

            TransactionTask restartFrag = createFrag(m_localTxnId[i]++, firstFragOfNextTxn[i].getTxnId(), m_txnTaskQueues.get(i));
            addTask(restartFrag, m_txnTaskQueues.get(i), m_expectedOrders.get(i));
        }

        // Finish the transaction, flush the backlog
        for (int i = 0; i < SITE_COUNT; i++) {
            firstFrag[i].getTransactionState().setDone();
            m_txnTaskQueues.get(i).flush(firstFrag[i].getTxnId());
        }

        verify();
    }


//    @Test
//    public void testBasicParticipantOps() throws InterruptedException
//    {
//        long localTxnId = 0;
//        long mpTxnId = 0;
//        SiteTaskerQueue task_queue = getSiteTaskerQueue();
//        TransactionTaskQueue dut = new TransactionTaskQueue(task_queue, 2);
//        Deque<TransactionTask> expected_order =
//            new ArrayDeque<TransactionTask>();
//
//        // add a few SP procs
//        TransactionTask next = createSpProc(localTxnId++, dut);
//        addTask(next, dut, expected_order);
//        next = createSpProc(localTxnId++, dut);
//        addTask(next, dut, expected_order);
//        next = createSpProc(localTxnId++, dut);
//        addTask(next, dut, expected_order);
//        // Should squirt on through the queue
//        assertEquals(0, dut.size());
//
//        // Now a fragment task to block things
//        long blocking_mp_txnid = mpTxnId;
//        next = createFrag(localTxnId++, mpTxnId++, dut);
//        TransactionTask block = next;
//        addTask(next, dut, expected_order);
//        assertEquals(1, dut.size());
//
//        // Add some tasks that are going to be blocked
//        // Manually track the should-be-blocked procedures
//        // for comparison later.
//        ArrayDeque<TransactionTask> blocked = new ArrayDeque<TransactionTask>();
//        next = createSpProc(localTxnId++, dut);
//        addTask(next, dut, blocked);
//        next = createSpProc(localTxnId++, dut);
//        addTask(next, dut, blocked);
//
//        // here's our next blocker
//        next = createFrag(localTxnId++, mpTxnId++, dut);
//        addTask(next, dut, blocked);
//        assertEquals(blocked.size() + 1, dut.size());
//
//        // Add a completion for the next blocker, too.  Simulates rollback causing
//        // an additional task for this TXN ID to appear before it's blocking the queue
//        next = createComplete(next.getTransactionState(), next.getTxnId(), dut);
//        addTask(next, dut, blocked);
//        assertEquals(blocked.size() + 1, dut.size());
//        System.out.println("blocked: " + blocked);
//
//        // now, do more work on the blocked task
//        next = createFrag(block.getTransactionState(), blocking_mp_txnid, dut);
//        addTask(next, dut, expected_order);
//        // Should have passed through and not be in the queue
//        assertEquals(blocked.size() + 1, dut.size());
//
//        // now, complete the blocked task
//        next = createComplete(block.getTransactionState(), blocking_mp_txnid, dut);
//        addTask(next, dut, expected_order);
//        // Should have passed through and not be in the queue
//        assertEquals(blocked.size() + 1, dut.size());
//        // DONE!  Should flush everything to the next blocker
//        block.getTransactionState().setDone();
//        int offered = dut.flush(block.getTxnId());
//        assertEquals(blocked.size(), offered);
//        assertEquals(1, dut.size());
//        expected_order.addAll(blocked);
//
//        while (!expected_order.isEmpty())
//        {
//            TransactionTask next_poll = (TransactionTask)task_queue.take();
//            TransactionTask expected = expected_order.removeFirst();
//            assertEquals(expected.getSpHandle(), next_poll.getSpHandle());
//            assertEquals(expected.getTxnId(), next_poll.getTxnId());
//        }
//    }
}
