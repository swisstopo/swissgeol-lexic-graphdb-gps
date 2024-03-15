package ch.swissgeol.gbgps.controller;

import ch.swissgeol.gbgps.service.Rdf4JService;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.RestPath;

import java.io.IOException;
import java.io.InputStream;

/**
 * Will publish the RDF file to graphdb repository
 */
@Path("/publish/{repository}")
public class PublishEndpoint {

    /** The logged */
    private static final Logger log = Logger.getLogger(PublishEndpoint.class);

    /** The rdf4j service */
    @Inject
    private Rdf4JService rdf4j;

    /**
     * Will publish the RDF file to graphdb repository
     * @param repository provided by rest path, contains the graphdb repository to be updated
     * @param context_url provided by query param, contains the graph context to be updated
     * @param content provided by request body, contains the RDF file to be pushed to graphdb
     * @return "OK" string if successful
     */
    @PUT
    @Produces(MediaType.TEXT_PLAIN)
    public String publish(
            @RestPath String repository,
            @QueryParam("context") String context_url,
            InputStream content
    ) {
        String tid = "["+System.currentTimeMillis()+"] ";

        log.info(String.format(
                tid + "Requested to publish on repository '{}' with context {}",
                repository, context_url));

        boolean res = false;
        try {
            res = rdf4j.publishRdf(
                    content,
                    repository,
                    context_url
            );
        } catch (IOException e) {
            log.error(tid + "Unable to publish the file! Message: " + e.getMessage());
            throw new jakarta.ws.rs.ServerErrorException(
                    "Unable to publish the file.",
                    Response.Status.INTERNAL_SERVER_ERROR,
                    e
            );
        }

        if (res)
            return "OK";
        else
            throw new ServerErrorException(
                    "Unable to publish the file.",
                    Response.Status.INTERNAL_SERVER_ERROR
            );
    }
}
