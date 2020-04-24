//package org.knime.core.data;
//
//import java.io.File;
//import java.util.Map;
//
//import org.knime.core.data.column.ColumnReadableTable;
//import org.knime.core.data.column.ColumnType;
//import org.knime.core.data.domain.Domain;
//import org.knime.core.data.record.CachedRecordStore;
//import org.knime.core.data.record.RecordFactory;
//import org.knime.core.data.record.RecordFormat;
//import org.knime.core.data.record.RecordStore;
//import org.knime.core.data.record.RecordUtils;
//import org.knime.core.data.record.RecordWriter;
//import org.knime.core.data.row.RowTableUtils;
//import org.knime.core.data.row.RowWriteCursor;
//import org.knime.core.data.row.RowWriteTable;
//import org.knime.core.data.value.DoubleWriteValue;
//import org.knime.core.data.value.WriteValue;
//
//public class APIMock {
//
//	public BufferedDataTable execute(DataTableSpec spec, ExecutionContext ctx) throws Exception {
//		final TableContainer container = ctx.createTableContainer(spec);
//		BufferedDataTable close = container.close();
//		return close;
//	}
//
//	public void readTableFromDisc() {
//		// deserialize file & domains from somewhere
//		File f = null;
//
//		// Deserialize from somewhere
//		Map<Long, Domain> domains = null;
//		ColumnType<?, ?>[] types = null;
//
//		// TODO make sure we load the right version of 'TableData'
//		RecordFormat store = null;
//
//		// TODO attach memory listeners
//		// TODO add to managed caches.
////		LoadingDataStore cache = new CachedDataReadStore(types, store.getReadAccess());
//
//		// we got our table back
//		// TODO we only need a 'Read' Cache here.
//		ColumnReadableTable table = RowTableUtils.create(cache);
//	}
//
//	// all in memory case
//	class ExecutionContext {
//		public TableContainer createTableContainer(DataTableSpec spec) {
//
//			// New temporary file
//			File f = null;
//
//			// new ArrowTableIO(), later e.g. from extension point
//			// TODO PrimitiveTypes are associcated with an array type.
//			// TODO Support for 'Grouped/Struct' native types (-> struct)
//			// TODO add config for individual columns (dict encoding, domain etc)
//			ColumnType<?, ?>[] types = translate(spec);
//
//			// Store to read/write data
//			// TODO create with primitive types. contract: delivers the correct
//			// loader/writer/factory according to PrimitiveTypes (e.g. double -> DoubleData,
//			// byte[] -> ByteArrayData, etc).
//
//			// TODO framework has to make sure to be able to serialize and deserialize the
//			// TableData object. We'll create TableDataV2, TableDataV3, ... in the future
//			// (backwards compatibility).
//			RecordFormat data = null;
//
//			// Create a new store which can actually deal with Read / Write. For
//			// deserialized tables we only need the 'read' aspect.
//			RecordStore store = data.create(types);
//
//			/*
//			 * Cache the store.
//			 * 
//			 * TODO register to global cache management (register to memory alerts, in case
//			 * of off-heap also to that).
//			 */
//			CachedRecordStore cached = RecordUtils.cache(store);
//
//			/*
//			 * Add preprocessors for data
//			 * 
//			 * TODO add unique value checker etc.
//			 * 
//			 * TODO async computation of adapters. data can be added to cached before all
//			 * adapters are ready (?).
//			 * 
//			 * TODO test design with implementing dictionary encoding either as adapter OR
//			 * in DataAccess? Do we need dictionary encoding at all for in-memory
//			 * representation or is parquet good enough?
//			 */
//
//			// general idea: this guy takes a recordbatch, splits it into columns and
//			// processes each column individually according to the assigned preprocessors
//			// which are just Consumer<RecordBatch>. When all parallel column workers are
//			// synced again we pass the record batch to the cache.
//			//
//			// IDEA: We could streamline the implementation and not pass a record batch to
//			// the
//			// cache, but the split column. as the cache likely caches each column
//			// individually anyways and just reassembles record batches as needed. Like that
//			// we wouldn't have to sync on all columns.
//
//			// Forseeable Problems:
//			// - If data is requested while domain is still calculated we have to block the
//			// cache (we can't fall back on writer, nothing has been written). Flush index
//			// can help to detect these situations, however, we would have to implement a
//			// flush index per column (which might be just fine).
//
//			// Preferred approach: Implementation without streamlining, learn something and
//			// refactor in second step as needed.
//
//			// Creates a writer to write columns of a table
//			// wrap table into a TableContainer for outside access
//			TableContainer container = new TableContainer() {
//
//				// TODO return whatever we declare as API here
//				// A table which can be filled with data.
////				private ColumnWriteTable columnWriteTable = TableUtils.createColumnWriteTable(adapted,
////						data.getFactory());
//
//				final RecordFactory factory = null;
//				final RecordWriter writer = null;
//
//				private RowWriteTable writeTable = RowTableUtils.createRowWriteTable(factory, writer);
//
//				// Similar to current API we close the container and with that create a
//				// BufferedDataTable.
//				@Override
//				public BufferedDataTable close() throws Exception {
//
//					final RowWriteCursor writeCursor = writeTable.cursor();
//					WriteValue writeAccess0 = writeCursor.get(0);
//					WriteValue writeAccess1 = writeCursor.get(1);
//					WriteValue writeAccess2 = writeCursor.get(2);
//					WriteValue writeAccess3 = writeCursor.get(3);
//
//					for (int i = 0; i < 1000; i++) {
//						writeCursor.fwd();
//						writeAccess0.setMissing();
//						writeAccess1.setMissing();
//						writeAccess2.setMissing();
//						writeAccess3.setMissing();
//					}
////
////					// you're not allowed to ever return a different access than this one.
////					DoubleWriteValue access = (DoubleWriteValue) rowCursor.get().getWriteAccess(0);
////					for (;;)
////						rowCursor.fwd();
//
//					// all data has been persisted. Close all writers!
//					// should already be closed (WriteColumn.close())
//
//					// TODO get reader from writer instead? reader=writer?
//					// TODO maybe the ArrayIO is versioned and NOT the reader/writers themselves.
//
////					ColumnReadableTable readTable = TableUtils.create(store, store.createReader());
//
//					// return some wrapped BufferedDataTable providing access to data, e.g. through
//					// ReadTable table = TableUtils.create(reader, null);
//					// TODO respect filters etc.
//					return null;
//				}
//			};
//
//			return null;
//		}
//
//		private ColumnType<?, ?>[] translate(DataTableSpec spec) {
//			return null;
//		}
//	}
//
//	/**
//	 * Execution Context point of view
//	 */
//
//	interface BufferedDataTable {
//		/* TODO whatever is our API here later with iteratorUnsafe etc */
//		ColumnReadableTable getReadTable();
//	}
//
//	interface DataTableSpec {
//
//	}
//
//	interface DataColumnSpec {
//
//	}
//
//	interface TableContainer {
//		BufferedDataTable close() throws Exception;
//	}
//}
