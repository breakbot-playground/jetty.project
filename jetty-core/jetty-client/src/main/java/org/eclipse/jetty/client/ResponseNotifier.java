//
// ========================================================================
// Copyright (c) 1995-2022 Mort Bay Consulting Pty Ltd and others.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License v. 2.0 which is available at
// https://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
// ========================================================================
//

package org.eclipse.jetty.client;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.io.Content;
import org.eclipse.jetty.io.content.ByteBufferContentSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponseNotifier
{
    private static final Logger LOG = LoggerFactory.getLogger(ResponseNotifier.class);

    public void notifyBegin(List<Response.ResponseListener> listeners, Response response)
    {
        for (Response.ResponseListener listener : listeners)
        {
            if (listener instanceof Response.BeginListener)
                notifyBegin((Response.BeginListener)listener, response);
        }
    }

    private void notifyBegin(Response.BeginListener listener, Response response)
    {
        try
        {
            listener.onBegin(response);
        }
        catch (Throwable x)
        {
            LOG.info("Exception while notifying listener {}", listener, x);
        }
    }

    public boolean notifyHeader(List<Response.ResponseListener> listeners, Response response, HttpField field)
    {
        boolean result = true;
        for (Response.ResponseListener listener : listeners)
        {
            if (listener instanceof Response.HeaderListener)
                result &= notifyHeader((Response.HeaderListener)listener, response, field);
        }
        return result;
    }

    private boolean notifyHeader(Response.HeaderListener listener, Response response, HttpField field)
    {
        try
        {
            return listener.onHeader(response, field);
        }
        catch (Throwable x)
        {
            LOG.info("Exception while notifying listener {}", listener, x);
            return false;
        }
    }

    public void notifyHeaders(List<Response.ResponseListener> listeners, Response response)
    {
        for (Response.ResponseListener listener : listeners)
        {
            if (listener instanceof Response.HeadersListener)
                notifyHeaders((Response.HeadersListener)listener, response);
        }
    }

    private void notifyHeaders(Response.HeadersListener listener, Response response)
    {
        try
        {
            listener.onHeaders(response);
        }
        catch (Throwable x)
        {
            LOG.info("Exception while notifying listener {}", listener, x);
        }
    }

    public void notifyContent(Response response, Content.Source contentSource, List<Response.ContentSourceListener> contentListeners)
    {
        int count = contentListeners.size();
        if (count == 0)
        {
            // Exactly 0 ContentSourceListener -> drive the read/demand loop internally here.
            driveDemandLoop(contentSource);
        }
        else if (count == 1)
        {
            // Exactly 1 ContentSourceListener -> notify it so that it drives the read/demand loop.
            Response.ContentSourceListener listener = contentListeners.get(0);
            notifyContent(listener, response, contentSource);
        }
        else
        {
            // 2+ ContentSourceListeners -> create a multiplexed content source and notify all listeners so that
            // they drive each a read/demand loop.
            Multiplexer multiplexer = new Multiplexer(contentSource, contentListeners.size());
            for (int i = 0; i < contentListeners.size(); i++)
            {
                Response.ContentSourceListener listener = contentListeners.get(i);
                notifyContent(listener, response, multiplexer.contentSource(i));
            }
        }
    }

    // TODO this class may need some form of thread-safety, but for now it is assumed the MultiplexerContentSource
    //  instances run strictly in sequence.
    private static class Multiplexer
    {
        private static final Logger LOG = LoggerFactory.getLogger(Multiplexer.class);

        private final Content.Source originalContentSource;
        private final MultiplexerContentSource[] multiplexerContentSources;
        private final Runnable[] demandCallbacks;
        private final Content.Chunk[] chunks;

        private Multiplexer(Content.Source originalContentSource, int size)
        {
            if (size < 2)
                throw new IllegalArgumentException("Multiplexer can only be used with a size >= 2");

            this.originalContentSource = originalContentSource;
            multiplexerContentSources = new MultiplexerContentSource[size];
            for (int i = 0; i < size; i++)
            {
                multiplexerContentSources[i] = new MultiplexerContentSource(i);
            }
            demandCallbacks = new Runnable[size];
            chunks = new Content.Chunk[size];
            if (LOG.isDebugEnabled())
                LOG.debug("Using multiplexer with a size of {}", size);
        }

        public Content.Source contentSource(int index)
        {
            return multiplexerContentSources[index];
        }

        private void demandIfAllRead()
        {
            boolean allRead = true;
            for (Content.Chunk chunk : chunks)
            {
                allRead &= chunk instanceof TombChunk;
            }

            if (allRead)
            {
                if (LOG.isDebugEnabled())
                    LOG.debug("All {} content sources read the current chunk, demanding", multiplexerContentSources.length);
                for (Runnable demandCallback : demandCallbacks)
                {
                    if (demandCallback != null)
                    {
                        if (LOG.isDebugEnabled())
                            LOG.debug("Demand has been registered by a multiplexed content source, demanding on the original content source");
                        originalContentSource.demand(this::onDemandCallback);
                        break;
                    }
                }
            }
        }

        private void onDemandCallback()
        {
            if (LOG.isDebugEnabled())
                LOG.debug("Original content source's demand calling back");

            Arrays.fill(chunks, null);
            Runnable[] demandCallbacksCopy = Arrays.copyOf(demandCallbacks, demandCallbacks.length);
            Arrays.fill(demandCallbacks, null);

            for (int i = 0; i < demandCallbacksCopy.length; i++)
            {
                Runnable demandCallback = demandCallbacksCopy[i];
                if (LOG.isDebugEnabled())
                    LOG.debug("Content source #{} registered callback {}", i, demandCallback);
                if (demandCallback != null)
                {
                    demandCallback.run();
                    if (LOG.isDebugEnabled())
                        LOG.debug("Called content source #{}'s callback", i);
                }
            }
        }

        Content.Chunk readCurrentChunk(int index)
        {
            if (chunks[index] instanceof TombChunk)
            {
                if (LOG.isDebugEnabled())
                    LOG.debug("Content source #{} already read current chunk", index);
                return null;
            }

            Content.Chunk result;
            if (chunks[index] == null)
            {
                if (LOG.isDebugEnabled())
                    LOG.debug("Current chunk is null, reading a new one from the original content source");
                Content.Chunk chunk = originalContentSource.read();
                for (int i = 0; i < chunks.length; i++)
                {
                    chunks[i] = Content.Chunk.slice(chunk);
                }
                result = chunks[index];
            }
            else
            {
                result = chunks[index];
                demandIfAllRead();
            }

            chunks[index] = TombChunk.INSTANCE;
            if (LOG.isDebugEnabled())
                LOG.debug("Content source #{} read current chunk: {}", index, result);
            return result;
        }

        void registerDemand(int index, Runnable demandCallback)
        {
            if (demandCallbacks[index] != null)
                throw new IllegalStateException();
            demandCallbacks[index] = demandCallback;

            boolean allDemanded = true;
            for (Runnable demand : demandCallbacks)
            {
                allDemanded &= demand != null;
            }

            if (allDemanded)
            {
                if (LOG.isDebugEnabled())
                    LOG.debug("All {} content sources demanded, demanding", multiplexerContentSources.length);
                originalContentSource.demand(this::onDemandCallback);
            }
        }

        private class MultiplexerContentSource implements Content.Source
        {
            private final int index;

            private MultiplexerContentSource(int index)
            {
                this.index = index;
            }

            @Override
            public Content.Chunk read()
            {
                return readCurrentChunk(index);
            }

            @Override
            public void demand(Runnable demandCallback)
            {
                registerDemand(index, demandCallback);
            }

            @Override
            public void fail(Throwable failure)
            {
                originalContentSource.fail(failure);
            }
        }

        private static class TombChunk implements Content.Chunk
        {
            static TombChunk INSTANCE = new TombChunk();

            @Override
            public ByteBuffer getByteBuffer()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isLast()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void retain()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean release()
            {
                throw new UnsupportedOperationException();
            }
        }
    }

    private void driveDemandLoop(Content.Source contentSource)
    {
        Content.Chunk read = contentSource.read();
        if (read != null)
            read.release();
        if (read == null || !read.isLast())
            contentSource.demand(() -> driveDemandLoop(contentSource));
    }

    private void notifyContent(Response.ContentSourceListener listener, Response response, Content.Source contentSource)
    {
        try
        {
            listener.onContentSource(response, contentSource);
        }
        catch (Throwable x)
        {
            LOG.info("Exception while notifying listener {}", listener, x);
        }
    }

    public void notifySuccess(List<Response.ResponseListener> listeners, Response response)
    {
        for (Response.ResponseListener listener : listeners)
        {
            if (listener instanceof Response.SuccessListener)
                notifySuccess((Response.SuccessListener)listener, response);
        }
    }

    private void notifySuccess(Response.SuccessListener listener, Response response)
    {
        try
        {
            listener.onSuccess(response);
        }
        catch (Throwable x)
        {
            LOG.info("Exception while notifying listener {}", listener, x);
        }
    }

    public void notifyFailure(List<Response.ResponseListener> listeners, Response response, Throwable failure)
    {
        for (Response.ResponseListener listener : listeners)
        {
            if (listener instanceof Response.FailureListener)
                notifyFailure((Response.FailureListener)listener, response, failure);
        }
    }

    private void notifyFailure(Response.FailureListener listener, Response response, Throwable failure)
    {
        try
        {
            listener.onFailure(response, failure);
        }
        catch (Throwable x)
        {
            LOG.info("Exception while notifying listener {}", listener, x);
        }
    }

    public void notifyComplete(List<Response.ResponseListener> listeners, Result result)
    {
        for (Response.ResponseListener listener : listeners)
        {
            if (listener instanceof Response.CompleteListener)
                notifyComplete((Response.CompleteListener)listener, result);
        }
    }

    private void notifyComplete(Response.CompleteListener listener, Result result)
    {
        try
        {
            listener.onComplete(result);
        }
        catch (Throwable x)
        {
            LOG.info("Exception while notifying listener {}", listener, x);
        }
    }

    public void forwardSuccess(List<Response.ResponseListener> listeners, Response response)
    {
        forwardEvents(listeners, response);
        notifySuccess(listeners, response);
    }

    public void forwardSuccessComplete(List<Response.ResponseListener> listeners, Request request, Response response)
    {
        forwardSuccess(listeners, response);
        notifyComplete(listeners, new Result(request, response));
    }

    public void forwardFailure(List<Response.ResponseListener> listeners, Response response, Throwable failure)
    {
        forwardEvents(listeners, response);
        notifyFailure(listeners, response, failure);
    }

    private void forwardEvents(List<Response.ResponseListener> listeners, Response response)
    {
        notifyBegin(listeners, response);
        Iterator<HttpField> iterator = response.getHeaders().iterator();
        while (iterator.hasNext())
        {
            HttpField field = iterator.next();
            if (!notifyHeader(listeners, response, field))
                iterator.remove();
        }
        notifyHeaders(listeners, response);
        if (response instanceof ContentResponse)
        {
            byte[] content = ((ContentResponse)response).getContent();
            if (content != null && content.length > 0)
            {
                List<Response.ContentSourceListener> contentListeners = listeners.stream()
                        .filter(Response.ContentSourceListener.class::isInstance)
                        .map(Response.ContentSourceListener.class::cast)
                        .toList();
                ByteBufferContentSource byteBufferContentSource = new ByteBufferContentSource(ByteBuffer.wrap(content));
                notifyContent(response, byteBufferContentSource, contentListeners);
            }
        }
    }

    public void forwardFailureComplete(List<Response.ResponseListener> listeners, Request request, Throwable requestFailure, Response response, Throwable responseFailure)
    {
        forwardFailure(listeners, response, responseFailure);
        notifyComplete(listeners, new Result(request, requestFailure, response, responseFailure));
    }
}
