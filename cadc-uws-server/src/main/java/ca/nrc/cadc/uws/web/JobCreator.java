/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2022.                            (c) 2022.
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
************************************************************************
 */

package ca.nrc.cadc.uws.web;

import ca.nrc.cadc.rest.SyncInput;
import ca.nrc.cadc.util.StringUtil;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobAttribute;
import ca.nrc.cadc.uws.JobInfo;
import ca.nrc.cadc.uws.Parameter;
import ca.nrc.cadc.uws.UWS;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.util.Streams;
import org.apache.log4j.Logger;

/**
 * Simple class used to read job description from the request and create a new Job.
 *
 * @author pdowler
 */
public class JobCreator {

    private static final Logger log = Logger.getLogger(JobCreator.class);

    protected static final DateFormat dateFormat = UWS.getDateFormat();

    protected static final String URLENCODED = "application/x-www-form-urlencoded";
    protected static final String TEXT_XML = "text/xml";
    protected static final String MULTIPART = "multipart/form-data";

    public JobCreator() {
    }

    // cadc-rest based create method
    public Job create(SyncInput input) {
        Job job = new Job();
        job.setExecutionPhase(ExecutionPhase.PENDING);
        job.setParameterList(new ArrayList<Parameter>());
        for (String pname : input.getParameterNames()) {
            if (JobAttribute.isValue(pname)) {
                processUWSParameter(job, pname, input.getParameter(pname));
            } else {
                processJobParameter(job, pname, input.getParameters(pname));
            }
        }
        log.warn("raw job params: " + job.getParameterList().size());
        for (Parameter p : job.getParameterList()) {
            log.warn("   raw: " + p.getName() + " = " + p.getValue());
        }
        for (String cname : input.getContentNames()) {
            log.warn("content name: " + cname);
            if (UWSInlineContentHandler.CONTENT_JOBINFO.equals(cname)) {
                JobInfo ji = (JobInfo) input.getContent(cname);
                job.setJobInfo(ji);
            } else if (UWSInlineContentHandler.CONTENT_PARAM_REPLACE.equals(cname)) {
                Object o = input.getContent(cname);
                List<UWSInlineContentHandler.ParameterReplacement> prs = new ArrayList();
                if (o instanceof List) {
                    List olist = (List) o;
                    for (Object ov : olist) {
                        prs.add((UWSInlineContentHandler.ParameterReplacement) ov);
                    }
                } else {
                    UWSInlineContentHandler.ParameterReplacement pr = (UWSInlineContentHandler.ParameterReplacement) o;
                    prs.add(pr);
                }
                for (UWSInlineContentHandler.ParameterReplacement pr : prs) {
                    boolean replaced = false;
                    for (Parameter p : job.getParameterList()) {
                        String ostr = p.getValue();
                        String nstr = ostr.replace(pr.origStr, pr.newStr);
                        if (!ostr.equals(nstr)) {
                            log.warn("replace param: " + p.getName() + ": " + ostr + " -> " + nstr);
                            p.setValue(nstr);
                            replaced = true;
                        }
                    }
                    if (!replaced) {
                        log.debug("no parameter modified by " + pr);
                    }
                }
            } else {
                log.warn("inline content: " + cname + " -> new param");
                // assume content -> param key=value
                Object val = input.getContent(cname);
                if (val != null) {
                    job.getParameterList().add(new Parameter(cname, val.toString()));
                }
            }
        }

        job.setRequestPath(input.getRequestPath());
        job.setRemoteIP(input.getClientIP());
        return job;
    }

    protected void processParameter(Job job, String name, String[] values) {
        if (JobAttribute.isValue(name)) {
            if (values != null && values.length > 0) {
                processUWSParameter(job, name, values[0]);
            }
        } else {
            processJobParameter(job, name, Arrays.asList(values));
        }
    }

    private void processUWSParameter(Job job, String name, String value) {
        if (name.equalsIgnoreCase(JobAttribute.RUN_ID.getValue())) {
            job.setRunID(value);
        } else if (name.equalsIgnoreCase(JobAttribute.DESTRUCTION_TIME.getValue())) {
            if (StringUtil.hasText(value)) {
                try {
                    job.setDestructionTime(dateFormat.parse(value));
                } catch (ParseException e) {
                    log.error("Cannot parse Destruction Time to IVOA date format " + value, e);
                    throw new IllegalArgumentException("Cannot parse Destruction Time to IVOA date format " + value, e);
                }
            } else {
                job.setDestructionTime(null);
            }
        } else if (name.equalsIgnoreCase(JobAttribute.EXECUTION_DURATION.getValue())) {
            if (StringUtil.hasText(value)) {
                job.setExecutionDuration(Long.parseLong(value));
            }
        } else if (name.equalsIgnoreCase(JobAttribute.QUOTE.getValue())) {
            if (StringUtil.hasText(value)) {
                try {
                    job.setQuote(dateFormat.parse(value));
                } catch (ParseException e) {
                    log.error("Cannot parse Quote to IVOA date format " + value, e);
                    throw new IllegalArgumentException("Cannot parse Quote to IVOA date format " + value, e);
                }
            } else {
                job.setQuote(null);
            }
        }
    }

    protected void processJobParameter(Job job, String name, Iterable<String> values) {
        for (String value : values) {
            job.getParameterList().add(new Parameter(name, value));
        }
    }

    protected void processMultiPart(Job job, FileItemIterator itemIterator, InlineContentHandler ich)
            throws FileUploadException, IOException {
        while (itemIterator.hasNext()) {
            FileItemStream item = itemIterator.next();
            String name = item.getFieldName();
            InputStream stream = item.openStream();
            if (item.isFormField()) {
                processParameter(job, name, new String[]{Streams.asString(stream)});
            } else {
                processStream(name, item.getContentType(), stream, ich);
            }
        }
    }

    protected void processStream(String name, String contentType, InputStream inputStream, InlineContentHandler ich)
            throws IOException {
        ich.accept(name, contentType, inputStream);
    }

}
