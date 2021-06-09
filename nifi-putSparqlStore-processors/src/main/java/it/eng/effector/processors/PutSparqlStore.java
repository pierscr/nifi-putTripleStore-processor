/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package it.eng.effector.processors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryLockedException;
import org.eclipse.rdf4j.repository.manager.LocalRepositoryManager;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tags({"ais"})
@TriggerSerially
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class PutSparqlStore extends AbstractProcessor {

    private static final Logger logger = LoggerFactory.getLogger(PutSparqlStore.class);


    public static final PropertyDescriptor REPO_DIR = new PropertyDescriptor
            .Builder().name("REPO_DIR")
            .displayName("RDF4J repositories folder")
            .description("The repository folder of rdf4j")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor REPO_ID = new PropertyDescriptor
            .Builder().name("REPO_ID")
            .displayName("RDF4J repository id")
            .description("The target repository id ")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Sucessfully mapped attribute names")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to map attribute names")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(REPO_DIR);
        descriptors.add(REPO_ID);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        try{
            final String flowFileId=String.valueOf(flowFile.getId());
            final String repoDir=context.getProperty(REPO_DIR).getValue();
            final String repoId=context.getProperty(REPO_ID).getValue();
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream inputStream) throws IOException {
                    //Repository repository = new SPARQLRepository(sparqlEndpoint);
                    File baseDir = new File(repoDir);
                    LocalRepositoryManager manager = new LocalRepositoryManager(baseDir);
                    Repository repository = manager.getRepository(repoId);
                   // SailRepository sail=new SailRepository(new MemoryStore());
                    repository.init();
                    IRI context=Values.iri("http://effector.com/flofile/"+flowFileId);
                    logger.debug("start processor: http://effector.com/flofile/"+flowFileId);
                    try(RepositoryConnection con=repository.getConnection()) {
                        logger.debug("get connection");
                        con.begin(IsolationLevels.SERIALIZABLE);
                        logger.debug("begin");
                        con.add(inputStream, RDFFormat.TURTLE);
                        logger.debug("add");
                        con.commit();
                        logger.debug("commit");
                        con.close();
                        repository.shutDown();
                        logger.debug("shutdown");
                    }

                }
            });
            logger.debug("transfer");
            session.transfer(flowFile, REL_SUCCESS);
        }catch (Exception e){
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(InputStream inputStream, OutputStream outputStream) throws IOException {
                    IOUtils.write( "{'error':'"+e.getMessage()+"','stack':'"+ ExceptionUtils.getStackTrace(e)+"'}", outputStream,  StandardCharsets.UTF_8);
                }
            });
            logger.error(ExceptionUtils.getStackTrace(e));
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
