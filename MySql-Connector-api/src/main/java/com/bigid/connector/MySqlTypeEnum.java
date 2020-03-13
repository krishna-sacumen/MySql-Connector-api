package com.bigid.connector;

public enum MySqlTypeEnum {
	INTEGER {
		public String toString() {
			return "INT";
		}
	},
	VARCHAR {
		public String toString() {
			return "VARCHAR";
		}
	},
	FLOAT {
		public String toString() {
			return "FLOAT";
		}
	},
	DOUBLE {
		public String toString() {
			return "DOUBLE";
		}
	},
	NUMERIC {
		public String toString() {
			return "NUMERIC";
		}
	},
	CHAR {
		public String toString() {
			return "CHAR";
		}
	},
	DATE {
		public String toString() {
			return "DATE";
		}
	},
	TIME {
		public String toString() {
			return "TIME";
		}
	},
	TIMESTAMP {
		public String toString() {
			return "TIMESTAMP";
		}
	},
}
