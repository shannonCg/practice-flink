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

package com.shaice.flink.fraudDetectorTest;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class FraudDetectorV2 extends KeyedProcessFunction<Long, Transaction, Alert> {

	private static final long serialVersionUID = 1L;

	private static final double SMALL_AMOUNT = 1.00;
	private static final double LARGE_AMOUNT = 500.00;
	private static final long ONE_MINUTE = 60 * 1000;

	private Integer id;
	private transient ValueState<Boolean> flagState;
	private transient ValueState<Long> timerState;

	@Override
	public void open(Configuration parameters) {
		ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>("flag", Types.BOOLEAN);
		flagState = getRuntimeContext().getState(flagDescriptor);

		ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>("timer-state", Types.LONG);
		timerState = getRuntimeContext().getState(timerDescriptor);
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out){
		flagState.clear();
		timerState.clear();
	}

	@Override
	public void processElement(
			Transaction transaction,
			Context context,
			Collector<Alert> collector) throws Exception {
		setId();
		System.out.println("thread"+id+" process transaction"+transaction);

		if(Objects.nonNull(flagState.value())){
			if(LARGE_AMOUNT < transaction.getAmount()){
				Alert alert = new Alert();
				alert.setId(transaction.getAccountId());
	
				collector.collect(alert);

			}
			cleanUp(context);
		}

		if(SMALL_AMOUNT > transaction.getAmount()){
			flagState.update(true);

			long timer = context.timerService().currentProcessingTime()+ONE_MINUTE;
			context.timerService().registerProcessingTimeTimer(timer);
			timerState.update(timer);
		}
	}

	private void setId(){
		if(Objects.isNull(id) || 0 == id){
			id = ThreadLocalRandom.current().nextInt();
		}
	}

	private void cleanUp(Context ctx) throws IOException{
		Long timer = timerState.value();
		ctx.timerService().deleteProcessingTimeTimer(timer);

		flagState.clear();
		timerState.clear();
	}
}
