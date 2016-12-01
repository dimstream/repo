package com.vmware.dim.entity;

import java.util.ArrayList;

/**
 * Object model Class which defines a join between the parent Stream and a given
 * Context, and enumerated the Javascript expressions depicting join conditions.
 *
 */
public class ContextJoin {
	String contextGlobalAlias;
	ArrayList<String> joinConditions;
}
