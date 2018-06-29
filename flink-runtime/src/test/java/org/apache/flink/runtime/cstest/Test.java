package org.apache.flink.runtime.cstest;

import org.apache.flink.runtime.clusterframework.types.ResourceID;

/**
 * @author chengshi
 * @function:
 * @create 2018/6/28 下午7:31
 */

public class Test {

	public static void main(String[] args) {
		ResourceID resourceID = ResourceID.generate();
		System.out.println(resourceID);
	}

}
