/*
 ************************************************************************
 ****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
 *
 * (c) 2012.                         (c) 2012.
 * National Research Council            Conseil national de recherches
 * Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
 * All rights reserved                  Tous droits reserves
 *
 * NRC disclaims any warranties         Le CNRC denie toute garantie
 * expressed, implied, or statu-        enoncee, implicite ou legale,
 * tory, of any kind with respect       de quelque nature que se soit,
 * to the software, including           concernant le logiciel, y com-
 * without limitation any war-          pris sans restriction toute
 * ranty of merchantability or          garantie de valeur marchande
 * fitness for a particular pur-        ou de pertinence pour un usage
 * pose.  NRC shall not be liable       particulier.  Le CNRC ne
 * in any event for any damages,        pourra en aucun cas etre tenu
 * whether direct or indirect,          responsable de tout dommage,
 * special or general, consequen-       direct ou indirect, particul-
 * tial or incidental, arising          ier ou general, accessoire ou
 * from the use of the software.        fortuit, resultant de l'utili-
 *                                      sation du logiciel.
 *
 *
 * @author jenkinsd
 * 4/20/12 - 12:44 PM
 *
 *
 *
 ****  C A N A D I A N   A S T R O N O M Y   D A T A   C E N T R E  *****
 ************************************************************************
 */
package ca.nrc.cadc.uws.web.restlet;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.CookiePrincipal;
import ca.nrc.cadc.auth.DelegationToken;
import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.auth.InvalidDelegationTokenException;
import ca.nrc.cadc.auth.PrincipalExtractor;
import ca.nrc.cadc.auth.SSOCookieCredential;
import ca.nrc.cadc.auth.SSOCookieManager;
import ca.nrc.cadc.auth.X509CertificateChain;
import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.util.ArrayUtil;
import ca.nrc.cadc.util.StringUtil;

import java.io.IOException;
import java.security.AccessControlException;
import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.log4j.Logger;
import org.restlet.Request;
import org.restlet.data.Cookie;
import org.restlet.data.Form;
import org.restlet.util.Series;


/**
 * Principal Extractor implementation using a Restlet Request.
 */
public class RestletPrincipalExtractor implements PrincipalExtractor
{
    private static final Logger log = Logger.getLogger(RestletPrincipalExtractor.class);
    
    private final Request request;
    private X509CertificateChain chain;
    private DelegationToken token;

    private List<SSOCookieCredential> cookieCredentialList;
    private Set<Principal> cookiePrincipals = new HashSet<>(); // identities extracted from cookie
    private Set<Principal> principals = new HashSet<>();

    /**
     * Hidden no-arg constructor for testing.
     */
    RestletPrincipalExtractor()
    {
        this.request = null;
    }

    /**
     * Create this extractor from the given Restlet Request.
     *
     * @param req       The Restlet Request.
     */
    public RestletPrincipalExtractor(final Request req)
    {
        this.request = req;
    }

    private void init()
    {
        if (chain == null)
        {
            final Collection<X509Certificate> requestCertificates =
                (Collection<X509Certificate>) getRequest().getAttributes().get(
                        "org.restlet.https.clientCertificates");
            if ((requestCertificates != null) && (!requestCertificates.isEmpty())) {
                this.chain = new X509CertificateChain(requestCertificates);
                if (this.chain != null) {
                    principals.add(this.chain.getPrincipal());
                }
            }

        }
        
        if (token == null)
        {
            log.info("adding token");
            Form headers = (Form) getRequest().getAttributes().get("org.restlet.http.headers");
            String tokenValue = headers.getFirstValue(AuthenticationUtil.AUTH_HEADER);
            if ( StringUtil.hasText(tokenValue) )
            {
                try
                {
                    this.token = DelegationToken.parse(tokenValue, request.getResourceRef().getPath());
                }
                catch (InvalidDelegationTokenException ex) 
                {
                    log.debug("invalid DelegationToken: " + tokenValue, ex);
                    throw new AccessControlException("invalid delegation token");
                }
                catch(RuntimeException ex)
                {
                    log.debug("invalid DelegationToken: " + tokenValue, ex);
                    throw new AccessControlException("invalid delegation token");
                }
                finally { }
            }
        }
log.info("adding principals");
        // add HttpPrincipal
        final String httpUser = getAuthenticatedUsername();
        if (StringUtil.hasText(httpUser)) // user from HTTP AUTH
            principals.add(new HttpPrincipal(httpUser));
        else if (token != null) // user from token
            principals.add(token.getUser());
        
        Series<Cookie> cookies = getRequest().getCookies();
        log.info("cookie count: " + cookies.size());
        log.info("principal count: " + principals.size());
        log.info(principals);
        if (cookies == null || (cookies.size() == 0))
            return;
        
        for (Cookie ssoCookie : cookies)
        {
            log.info(ssoCookie.toString());

            if (SSOCookieManager.DEFAULT_SSO_COOKIE_NAME.equals(
                    ssoCookie.getName())
                    && StringUtil.hasText(ssoCookie.getValue()))
            {
                SSOCookieManager ssoCookieManager = new SSOCookieManager();
                try
                {
                    DelegationToken cookieToken = ssoCookieManager.parse(
                        ssoCookie.getValue());

                    cookiePrincipals = cookieToken.getIdentityPrincipals();
                    principals.addAll(cookiePrincipals);

                    cookieCredentialList = ssoCookieManager.getSSOCookieCredentials(ssoCookie.getValue(),
                        NetUtil.getDomainName(getRequest().getResourceRef().toUrl()));
                } 
                catch (IOException e)
                {
                    log.info("Cannot use SSO Cookie. Reason: " 
                            + e.getMessage());
                } 
                catch (InvalidDelegationTokenException e)
                {
                    log.info("Cannot use SSO Cookie. Reason: " 
                            + e.getMessage());
                }
                
            }
        }
    }

    public X509CertificateChain getCertificateChain()
    {
        init();
        return chain;
    }

    public Set<Principal> getPrincipals()
    {
        init();
        return principals;
    }

    public DelegationToken getDelegationToken() 
    {
        init();
        return token;
    }

    /**
     * Obtain the Username submitted with the Request.
     *
     * @return      String username, or null if none found.
     */
    protected String getAuthenticatedUsername()
    {
        final String username;

//        log.info("getrequest object: " + request);
//        Request req = getRequest();
//        log.info("req: "  + req);
//        log.info("client info: " + req.getClientInfo());
//        log.info("principals: " + req.getClientInfo().getPrincipals());
        if (!getRequest().getClientInfo().getPrincipals().isEmpty())
        {
            // Put in to support Safari not injecting a Challenge Response.
            // Grab the first principal's name as the username.
            // update: this is *always* right and works with realms; the previous
            // call to getRequest().getChallengeResponse().getIdentifier() would
            // return whatever username the caller provided in a non-authenticating call
            username = getRequest().getClientInfo().getPrincipals().get(0).getName();
            log.info("username: " + username);
        }
        else 
        {
            username = null;
        }

        return username;
    }


    public Request getRequest()
    {
        return request;
    }

    @Override
    public List<SSOCookieCredential> getSSOCookieCredentials()
    {
       return cookieCredentialList;
    }
}
