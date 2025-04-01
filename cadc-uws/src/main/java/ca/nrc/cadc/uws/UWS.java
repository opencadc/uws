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

package ca.nrc.cadc.uws;

import ca.nrc.cadc.date.DateUtil;
import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.ParsePosition;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.jdom2.Namespace;

/**
 * Holder of commonly used constants.
 *
 * @author zhangsa
 *
 */
public abstract class UWS {
    public static final String UWS_XSD_FILE = "UWS-v1.1.xsd"; // local xsd file name
    public static final String UWS_NAMESPACE = "http://www.ivoa.net/xml/UWS/v1.0";
    public static final String UWS_VERSION = "1.1";

    public static final String XLINK_NAMESPACE = "http://www.w3.org/1999/xlink";
    public static final String XLINK_XSD_FILE = "XLINK.xsd";

    public static final Namespace NS = Namespace.getNamespace("uws", UWS_NAMESPACE);
    public static final Namespace XLINK_NS = Namespace.getNamespace("xlink", "http://www.w3.org/1999/xlink");
    public static final Namespace XSI_NS = Namespace.getNamespace("xsi", "http://www.w3.org/2001/XMLSchema-instance");

    public static final String UWS_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    public static final DateFormat getDateFormat() {
        return new MultiDateFormat();
    }

    // possibly temporary class to support multiple input formats for backwards compatibility
    private static class MultiDateFormat extends DateFormat {

        final DateFormat outputFmt = DateUtil.getDateFormat(UWS_DATE_FORMAT, DateUtil.UTC);
        final List<DateFormat> inputFmts = new ArrayList<>();

        MultiDateFormat() {
            inputFmts.add(outputFmt);
            inputFmts.add(DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC));
        }

        @Override
        public StringBuffer format(Date date, StringBuffer sb, FieldPosition fp) {
            return outputFmt.format(date, sb, fp);
        }

        @Override
        public Date parse(String string, ParsePosition pp) {
            for (DateFormat df : inputFmts) {
                Date ret = df.parse(string, pp);
                if (ret != null) {
                    return ret;
                }
            }
            return null;
        }
    }
}
