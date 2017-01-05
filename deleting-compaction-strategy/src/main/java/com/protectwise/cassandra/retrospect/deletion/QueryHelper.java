/*
 * Copyright 2016 ProtectWise, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.protectwise.cassandra.retrospect.deletion;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

public class QueryHelper
{
    private static final Logger logger = LoggerFactory.getLogger(QueryHelper.class);

    public static boolean hasStartedCQL()
    {
        return     Gossiper.instance.isEnabled()
                && Gossiper.instance.isKnownEndpoint(DatabaseDescriptor.getBroadcastRpcAddress())
                && seenAnySeedOrIsOnlySeed();
    }

    public static boolean isSeed()
    {
        for (InetAddress seed : DatabaseDescriptor.getSeeds())
        {
            if (seed.equals(FBUtilities.getBroadcastAddress()))
                return true;
        }
        return false;
    }

    /**
     * Test for whether a remote seed has been seen, or true if local is a seed and there are no remote seeds.
     * @return
     */
    public static boolean seenAnySeedOrIsOnlySeed()
    {
        return
               Gossiper.instance.seenAnySeed()
            || DatabaseDescriptor.getSeeds().size() == 1 && isSeed();

    }

}
