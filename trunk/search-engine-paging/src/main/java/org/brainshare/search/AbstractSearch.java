package org.brainshare.search;

import static org.hibernate.search.reader.ReaderProviderHelper.getIndexReaders;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.engine.SessionImplementor;
import org.hibernate.search.SearchException;
import org.hibernate.search.engine.DocumentBuilderIndexedEntity;
import org.hibernate.search.engine.SearchFactoryImplementor;
import org.hibernate.search.filter.FullTextFilterImplementor;
import org.hibernate.search.reader.ReaderProvider;
import org.hibernate.search.store.DirectoryProvider;
import org.hibernate.search.store.IndexShardingStrategy;
import org.hibernate.search.util.ContextHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"unchecked", "deprecation"})
public class AbstractSearch<T> {
	
	protected Class<T> tClass;
	
	final static Logger LOG = LoggerFactory.getLogger(AbstractSearch.class);
	
	public AbstractSearch() {
		this.tClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
	}

	public List<T> list(Session session, Query query, final int from, final int maxResults) throws HibernateException {
		SearchFactoryImplementor searchFactoryImplementor = ContextHelper.getSearchFactoryBySFI((SessionImplementor) session);
		List<DirectoryProvider> targetedDirectories = new ArrayList<DirectoryProvider>();
		Map<Class<?>, DocumentBuilderIndexedEntity<?>> builders = searchFactoryImplementor.getDocumentBuildersIndexedEntities();
		for (DocumentBuilderIndexedEntity builder : builders.values()) {
			populateDirectories(targetedDirectories, builder);
		}
		final DirectoryProvider[] directoryProviders = targetedDirectories.toArray(new DirectoryProvider[targetedDirectories.size()]);
		IndexSearcher searcher = new IndexSearcher(searchFactoryImplementor.getReaderProvider().openReader(directoryProviders));

		try {
			TopDocs topDocs = searcher.search(query, Integer.MAX_VALUE);
			ScoreDoc[] scoreDocs = topDocs.scoreDocs;
			int resultSize = topDocs.totalHits;
			int fromIndex = from;
			if (fromIndex > resultSize) {
				fromIndex = 0;
			}
			
			List<T> tList = new ArrayList<T>();
			Document document;
			T tObject;
			for(int i=0;i<maxResults && i+fromIndex<resultSize;i++) {
				document = searcher.doc(scoreDocs[i+fromIndex].doc);
				try {
					tObject = (T) session.get(tClass, Long.parseLong(document.get("id")));
					if(tObject != null) {
						tList.add(tObject);
					} else {
						LOG.warn("Object " + document.get("id") + " not found in database, lucene index propably needs reindexing.");
					}
				} catch(NumberFormatException e) {
					LOG.error("Malformed object id from Lucene index.", e);
				}
			}
			
			return tList;
		} catch (Exception e) {
			LOG.error("Unable to read query Lucene index.", e);
			return new ArrayList<T>(0);
		} finally {
			closeSearcher(query, searchFactoryImplementor, searcher);
		}
		
	}
	
	public void closeSearcher(Object query, SearchFactoryImplementor searchFactoryImplementor, IndexSearcher searcher) {
		Set<IndexReader> indexReaders = getIndexReaders( searcher );
		ReaderProvider readerProvider = searchFactoryImplementor.getReaderProvider();
		for ( IndexReader indexReader : indexReaders ) {
			try {
				readerProvider.closeReader( indexReader );
			}
			catch (SearchException e) {
				LOG.error("Cannot close Lucene Index Reader", e);
			}
		}
	}

	private void populateDirectories(List<DirectoryProvider> directories, DocumentBuilderIndexedEntity builder) {
		final IndexShardingStrategy indexShardingStrategy = builder.getDirectoryProviderSelectionStrategy();
		final DirectoryProvider[] directoryProviders;
		directoryProviders = indexShardingStrategy.getDirectoryProvidersForQuery( new FullTextFilterImplementor[0] );

		for ( DirectoryProvider provider : directoryProviders ) {
			if ( !directories.contains( provider ) ) {
				directories.add( provider );
			}
		}
	}

}
