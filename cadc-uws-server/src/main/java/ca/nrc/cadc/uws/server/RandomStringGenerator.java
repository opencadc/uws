/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2009.                            (c) 2009.
*  Government of Canada                 Gouvernement du Canada
*  National Research Council            Conseil national de recherches
*  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
*  All rights reserved                  Tous droits réservés
*
*  NRC disclaims any warranties,        Le CNRC dénie toute garantie
*  expressed, implied, or               énoncée, implicite ou légale,
*  statutory, of any kind with          de quelque nature que ce
*  respect to the software,             soit, concernant le logiciel,
*  including without limitation         y compris sans restriction
*  any warranty of merchantability      toute garantie de valeur
*  or fitness for a particular          marchande ou de pertinence
*  purpose. NRC shall not be            pour un usage particulier.
*  liable in any event for any          Le CNRC ne pourra en aucun cas
*  damages, whether direct or           être tenu responsable de tout
*  indirect, special or general,        dommage, direct ou indirect,
*  consequential or incidental,         particulier ou général,
*  arising from the use of the          accessoire ou fortuit, résultant
*  software.  Neither the name          de l'utilisation du logiciel. Ni
*  of the National Research             le nom du Conseil National de
*  Council of Canada nor the            Recherches du Canada ni les noms
*  names of its contributors may        de ses  participants ne peuvent
*  be used to endorse or promote        être utilisés pour approuver ou
*  products derived from this           promouvoir les produits dérivés
*  software without specific prior      de ce logiciel sans autorisation
*  written permission.                  préalable et particulière
*                                       par écrit.
*
*  This file is part of the             Ce fichier fait partie du projet
*  OpenCADC project.                    OpenCADC.
*
*  OpenCADC is free software:           OpenCADC est un logiciel libre ;
*  you can redistribute it and/or       vous pouvez le redistribuer ou le
*  modify it under the terms of         modifier suivant les termes de
*  the GNU Affero General Public        la “GNU Affero General Public
*  License as published by the          License” telle que publiée
*  Free Software Foundation,            par la Free Software Foundation
*  either version 3 of the              : soit la version 3 de cette
*  License, or (at your option)         licence, soit (à votre gré)
*  any later version.                   toute version ultérieure.
*
*  OpenCADC is distributed in the       OpenCADC est distribué
*  hope that it will be useful,         dans l’espoir qu’il vous
*  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
*  without even the implied             GARANTIE : sans même la garantie
*  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
*  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
*  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
*  General Public License for           Générale Publique GNU Affero
*  more details.                        pour plus de détails.
*
*  You should have received             Vous devriez avoir reçu une
*  a copy of the GNU Affero             copie de la Licence Générale
*  General Public License along         Publique GNU Affero avec
*  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
*  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
*                                       <http://www.gnu.org/licenses/>.
*
*  $Revision: 4 $
*
************************************************************************
 */

package ca.nrc.cadc.uws.server;

import ca.nrc.cadc.util.HexUtil;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import org.apache.log4j.Logger;

/**
 * String identifier generator. This class uses the <code>java.security.SecureRandom</code>
 * class to generater random identifiers from a set of characters (typically alphanumeric).
 * It always makes sure the first character is an alphabetic letter.
 *
 * @author pdowler
 */
public class RandomStringGenerator implements StringIDGenerator {

    private static Logger log = Logger.getLogger(RandomStringGenerator.class);

    // generate a random modest-length lower case string
    private static final String DEFAULT_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789";

    // shared random number generator for jobID generation
    private final SecureRandom rnd = new SecureRandom();
    private int length;
    private char[] characters;
    private int numLetters;

    /**
     * Constructor. Generates identifiers with the specified number of characters and
     * the default set of allowed characters: lower case letters [a-z] and digits [0-9].
     *
     * @param length
     */
    public RandomStringGenerator(int length) {
        this(length, DEFAULT_CHARS);
    }

    /**
     * Constructor. Generates identifiers with the specified number of characters and
     * the specified set of allowed characters.
     *
     * @param length
     * @param allowedChars
     */
    public RandomStringGenerator(int length, String allowedChars) {
        this.length = length;
        this.characters = new char[allowedChars.length()];

        // find/add all the letters first
        this.numLetters = 0;
        for (int i = 0; i < allowedChars.length(); i++) {
            char c = allowedChars.charAt(i);
            if (Character.isLetter(c)) {
                characters[i] = c;
                numLetters++;
            }
        }
        // find/add all the non-letters (presumably digits)
        int n = 0;
        for (int i = 0; i < allowedChars.length(); i++) {
            char c = allowedChars.charAt(i);
            if (!Character.isLetter(c)) {
                characters[numLetters + n] = c;
                n++;
            }
        }
        log.debug("allowed characters: " + new String(characters));
        log.debug("number of letters: " + numLetters);
        initRNG();
    }

    /**
     * Generate a new ID. This method is thread-safe.
     *
     * @return a new ID
     */
    public String getID() {
        synchronized (rnd) {
            char[] c = new char[length];
            c[0] = characters[rnd.nextInt(numLetters)];
            for (int i = 1; i < length; i++) {
                c[i] = characters[rnd.nextInt(characters.length)];
            }
            return new String(c);
        }
    }

    // package access for test code
    void initRNG() {
        // add extra seed info: clock
        byte[] clock = HexUtil.toBytes(System.currentTimeMillis());
        byte[] addr = null;
        try {
            // add extra seed info: ip address
            InetAddress inet = InetAddress.getLocalHost();
            if (!inet.isLoopbackAddress()) {
                addr = inet.getAddress();
            }
        } catch (UnknownHostException ignore) {
            log.debug("OOPS - failed to find hostname", ignore);
        }

        if (clock != null) {
            rnd.setSeed(clock);
        }
        if (addr != null) {
            rnd.setSeed(addr);
        }
    }
}
