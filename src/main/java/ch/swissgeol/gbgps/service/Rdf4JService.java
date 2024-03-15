package ch.swissgeol.gbgps.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.manager.RemoteRepositoryManager;
import org.eclipse.rdf4j.repository.manager.RepositoryManager;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

/**
 * Interacts with graphdb by rdf4j API
 */
@ApplicationScoped
public class Rdf4JService {

    /** The logger */
    private static final Logger log = Logger.getLogger(Rdf4JService.class);

    /** The url of the graphdb repository */
    @ConfigProperty(name = "swissgeol.graphdb.url", defaultValue = "http://localhost:5000")
    private String repository_url;

    /** The username of the graphdb repository */
    @ConfigProperty(name = "swissgeol.graphdb.username")
    private Optional<String> repository_username;

    /** The password of the graphdb repository */
    @ConfigProperty(name = "swissgeol.graphdb.password")
    private Optional<String> repository_password;

    /** The repository manager */
    private RepositoryManager repositoryManager;

    /** The initializer will create connection to graphdb */
    @PostConstruct
    public void init() {
        if (!repository_username.orElse("").trim().isEmpty()) {
            log.info("Initializing the repository manager with user " + repository_username);
            repositoryManager = RemoteRepositoryManager.getInstance(
                    repository_url,
                    repository_username.get(),
                    repository_password.orElse("")
            );
        } else {
            log.info("Initializing the repository manager with anonymous");
            repositoryManager = RemoteRepositoryManager.getInstance(
                    repository_url
            );
        }
    }

    /** The destroyer will drop connection to graphdb */
    @PreDestroy
    public void destroy() {
        if (repositoryManager != null) {
            log.info("Destroy repository manager");
            repositoryManager.shutDown();
        }
    }

    /**
     * Publish rdf content to graphdb
     * @param content the content to be published
     * @param repository_name the name of the repository
     * @param context_url the graph context url
     * @return if the public is successful
     * @throws IOException in case of IO errors
     */
    public boolean publishRdf(InputStream content, String repository_name, String context_url) throws IOException {

        String tid = "["+System.currentTimeMillis()+"] ";

        log.info(tid+"Start publishing rdf to repository "+repository_name+" for graph url: "+context_url);

        Repository repository = repositoryManager.getRepository(repository_name);
        RepositoryConnection connection = repository.getConnection();

        ValueFactory vf = connection.getValueFactory();

        log.info(tid+"Upload rdf to repository "+repository_name+" for graph url: "+context_url);

        connection.add(
                content,
                context_url,
                RDFFormat.RDFXML,
                vf.createIRI(context_url));

        log.info(tid+"Closing connection after publish to repository "+repository_name+" for graph url: "+context_url);

        connection.close();
        repository.shutDown();

        log.info(tid+"Connection closed after publish to repository "+repository_name+" for graph url: "+context_url);

        return true;
    }

}
