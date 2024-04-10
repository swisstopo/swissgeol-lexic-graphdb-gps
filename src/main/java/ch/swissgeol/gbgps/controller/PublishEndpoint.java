package ch.swissgeol.gbgps.controller;

import ch.swissgeol.gbgps.service.Rdf4JService;
import io.quarkus.security.UnauthorizedException;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.RestPath;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

/**
 * Will publish the RDF file to graphdb repository
 */
@Path("/publish/{repository}")
public class PublishEndpoint {

    /** The logged */
    private static final Logger log = Logger.getLogger(PublishEndpoint.class);

    @ConfigProperty(name = "swissgeol.graphdb-gps.token")
    private Optional<String> authorization_token;

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
            @HeaderParam("Authorization") String authorization_header,
            InputStream content
    ) {
        
        if (authorization_token.isPresent() && !StringUtils.isBlank(authorization_token.get())) {
            String auth_token = authorization_token.get();
            if (
                    authorization_header.length() <= 7
                            || !authorization_header.startsWith("Bearer ")
                            || !auth_token.equals(authorization_header.substring(7))
            ) {
                throw new UnauthorizedException("Invalid authorization token");
            }
        }
        
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
