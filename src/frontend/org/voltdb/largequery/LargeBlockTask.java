/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.largequery;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

public abstract class LargeBlockTask implements Callable<LargeBlockResponse> {
    static LargeBlockTask getStoreTask(BlockId blockId, long origAddress, ByteBuffer block) {
        return new LargeBlockTask() {
            @Override
            public LargeBlockResponse call() throws Exception {
                Exception theException = null;
                try {
                    LargeBlockManager.getInstance().storeBlock(blockId, origAddress, block);
                }
                catch (Exception exc) {
                    theException = exc;
                }

                return new LargeBlockResponse(theException);
            }
        };
    }

    static LargeBlockTask getReleaseTask(BlockId blockId) {
        return new LargeBlockTask() {
            @Override
            public LargeBlockResponse call() throws Exception {
                Exception theException = null;
                try {
                    LargeBlockManager.getInstance().releaseBlock(blockId);
                }
                catch (Exception exc) {
                    theException = exc;
                }

                return new LargeBlockResponse(theException);
            }
        };
    }
}
