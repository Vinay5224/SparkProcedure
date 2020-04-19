package com.procedures.SparkProcedures;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;

import com.procedures.config.SparkConfigurations;

public class Procedure1 {

	private static SparkConfigurations sparkConfigurations = SparkConfigurations.getInstance();

	public static void main(String[] args) {
		
		SparkSession sp = SparkConfigurations.createSession();
		Dataset<Row> customer = sp.read().jdbc(sparkConfigurations.getHost(), "CUSTOMER", SparkConfigurations.getProps()).limit(10);
		customer.createOrReplaceTempView("CUSTOMER");
		Dataset<Row> order = sp.read().jdbc(sparkConfigurations.getHost(), "OORDER", SparkConfigurations.getProps()).limit(10);
		order.createOrReplaceTempView("OORDER");
		Dataset<Row> orderLine = sp.read().jdbc(sparkConfigurations.getHost(), "ORDER_LINE", SparkConfigurations.getProps()).limit(10);
		orderLine.createOrReplaceTempView("ORDER_LINE");
		/*
		 * Note: Not adding 'NULL AS PLAN_RANK' COLUMN IN THIS JOIN
		 */
		Dataset<Row> stgFctMbrSptBefore = sp.sql("SELECT C.C_W_ID,	C.C_D_ID,C.C_ID,CASE WHEN C.C_LAST = '' THEN NULL ELSE C_LAST END AS C_LAST,"
											+"O.O_CARRIER_ID,O.O_C_ID,OL.OL_NUMBER,OL.OL_QUANTITY,CASE WHEN C.C_FIRST = '' THEN NULL ELSE C_FIRST END AS "
											+ "C_FIRST,C.C_STATE,OL_DELIVERY_D,NULL AS COB_PERSON_ID,NULL AS INSURED_ID,NULL "
											+ "AS COB_HIER FROM CUSTOMER AS C JOIN OORDER AS O ON C.C_W_ID = O.O_W_ID LEFT JOIN ORDER_LINE AS OL ON "
											+ "OL.OL_D_ID = C.C_D_ID LIMIT 5");  
		stgFctMbrSptBefore.createOrReplaceTempView("Stg_FactMember_Split_Before");
	
		//PROCEDURE 2 (Update happens based on Left join and ISNULL() Function is used);
		Dataset<Row> history = sp.read().jdbc(sparkConfigurations.getHost(), "HISTORY", SparkConfigurations.getProps()).limit(10);
		history.createOrReplaceTempView("HISTORY");
		
		stgFctMbrSptBefore = sp.sql("SELECT T1.* FROM Stg_FactMember_Split_Before AS T1 LEFT JOIN HISTORY H1 ON H1.H_C_W_ID = T1.C_W_ID");
		//ADDED PLAN RANK COLUMN WITH 14 HERE
		stgFctMbrSptBefore= stgFctMbrSptBefore.withColumn("PLAN_RANK", functions.lit(14));
		
		//DROPING ALL THE TEMP TABLES 
		//spark.sql("drop view hvac"); Similar function
		sp.catalog().dropTempView("CUSTOMER");
		sp.catalog().dropTempView("OORDER");
		sp.catalog().dropTempView("ORDER_LINE");
		sp.catalog().dropTempView("Stg_FactMember_Split_Before");
		
		
		//PROCEDURE 3
		 Dataset<Row> dates_input = sp.read().jdbc(sparkConfigurations.getHost(), "DATES", SparkConfigurations.getProps()).where("SD<DATEADD(DAY, -(DAY(ED) -1),ED)").limit(10);
		 dates_input.createOrReplaceTempView("DATES");
		 stgFctMbrSptBefore = sp.sql("PASTE THE  UNION ALL QUERY HERE");
		 stgFctMbrSptBefore.persist(StorageLevel.MEMORY_AND_DISK_SER());
		 
		 //PROCEDURE 4
		 Dataset<Row> stgFactMemberSplitsAfter = stgFctMbrSptBefore; //Filter the required columns here
		 stgFactMemberSplitsAfter.createOrReplaceTempView("Stg_FactMember_Splits_After");
		 
		 //PROCEDURE 5 : Implementing this Step in Reverse Logic 
		 stgFactMemberSplitsAfter = sp.sql("SELECT * FROM Stg_FactMember_Splits_After WHERE DAY([START_DATE]) BETWEEN 1 AND 15 AND " 
				 	+" COVERAGE_MONTH <> YEAR([START_DATE])*100+MONTH([START_DATE])");
		 sp.catalog().dropTempView("Stg_FactMember_Splits_After");
		 stgFactMemberSplitsAfter.createOrReplaceTempView("Stg_FactMember_Splits_After");
		 
		 //PROCEDURE 6 : Implementing this Step in Reverse Logic
		 stgFactMemberSplitsAfter = sp.sql("SELECT * FROM Stg_FactMember_Splits_After WHERE DAY(STOP_DATE) BETWEEN 15 AND 31 AND "
				 	+" COVERAGE_MONTH<>YEAR([STOP_DATE])*100+MONTH([STOP_DATE])	");
		 sp.catalog().dropTempView("Stg_FactMember_Splits_After");
		 stgFactMemberSplitsAfter.createOrReplaceTempView("Stg_FactMember_Splits_After");
		 
		 //PROCEDURE 7 
		 

		 //PROCEDURE 8
		 
	}

}
