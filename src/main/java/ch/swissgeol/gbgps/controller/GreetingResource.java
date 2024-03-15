package ch.swissgeol.gbgps.controller;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/**
 * Returns "OK". Could be used for healthcheck
 */
@Path("/health")
public class GreetingResource {

    /**
     * Returns "OK". Could be used for healthcheck
     * @return "OK" string
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {
        return "OK";
    }
}
