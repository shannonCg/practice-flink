/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.shaice.flink;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

	private static final long serialVersionUID = 1L;

	private static final double SMALL_AMOUNT = 1.00;
	private static final double LARGE_AMOUNT = 500.00;
	private static final long ONE_MINUTE = 60 * 1000;

	private Integer id;
	private Map<Long,Integer> accountFraudTimes = new HashMap<>();

	@Override
	public void processElement(
			Transaction transaction,
			Context context,
			Collector<Alert> collector) throws Exception {
		setId();
		System.out.println("thread"+id+" process transaction"+transaction);

		Long accountId = transaction.getAccountId();
		if(false == accountFraudTimes.containsKey(accountId)){
			accountFraudTimes.put(accountId, 0);
		}

		Integer fraudTimes = accountFraudTimes.get(accountId);
		if(SMALL_AMOUNT > transaction.getAmount() && fraudTimes == 0){
			fraudTimes++;
		}else if (LARGE_AMOUNT < transaction.getAmount() && fraudTimes == 1){
			fraudTimes++;
		}else{
			if(fraudTimes > 0){
				fraudTimes--;
			}
		}
		
		if(fraudTimes >= 2){
			Alert alert = new Alert();
			alert.setId(transaction.getAccountId());
	
			collector.collect(alert);

			fraudTimes = 0;
		}
		accountFraudTimes.put(accountId, fraudTimes);
	}

	private void setId(){
		if(Objects.isNull(id) || 0 == id){
			id = ThreadLocalRandom.current().nextInt();
		}
	}
}
