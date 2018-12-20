# uws
client and server implementations of the Universal Worker Service (UWS) specification


Build status:
<a href="https://travis-ci.org/opencadc/uws"><img src="https://travis-ci.org/opencadc/uws.svg?branch=master" /></a>

Compatibility Notes for cadc-uws-server-1.2 (now in master):

* If you implement/extend a JobRunner, the SyncOutput import has to be changed to use the one in the ca.nrc.cadc.rest package

* If you implemented an InlineContentHandler, the API for that changed; there is an extended interface UWSInlineContentHandler that defines some constants and a support class. For inline content that goes into the jonInfo field of a job, just return a Content instance with name = CONTENT_JOBINFO and value = new JobInfo(...) (e.g. vospace transfer document). A Content instance with name = CONTENT_PARAM_REPLACE and value = new ParameterReplacement(...) will tell the library to replace part of an existing parameter value with a new string. This is intended to support the TAP UPLOAD usage where UPLOAD=foo,param:bar: when the multipart content named bar is processed, you could for example store the content someplace and then replace "param:bar" with the URL to that other location. There is no direct access to the parameter list but we could easily add the ability to return new parameter(s) and have them added... TBD.

* The servlet setup in web.xml has also changed; see the web.xml in example-uws for how to use the JobServlet for async and sync endpoints.

* The JobManager interface was ruthlessly changed to include an extra argument (requestPath) in all methods; the SimpleJobManager accepts but ignores it so if you extended it you code will work fine with a recompile. There is a new RequestPathJobManager that allows an extension to instantiate different JobPersistence and JobExecutor instances on different paths; the intent is to allow for one servlet instance to segregate jobs by path (e.g. to create a set of TAP services with different back end servers or databases within one web service implementation). 
