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

package org.eclipse.jetty.http3.client.transport.internal;

import org.eclipse.jetty.client.HttpExchange;
import org.eclipse.jetty.client.HttpReceiver;
import org.eclipse.jetty.client.HttpResponse;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.MetaData;
import org.eclipse.jetty.http3.api.Stream;
import org.eclipse.jetty.http3.frames.HeadersFrame;
import org.eclipse.jetty.io.Content;
import org.eclipse.jetty.util.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpReceiverOverHTTP3 extends HttpReceiver implements Stream.Client.Listener
{
    private static final Logger LOG = LoggerFactory.getLogger(HttpReceiverOverHTTP3.class);

    private ContentSource contentSource;

    protected HttpReceiverOverHTTP3(HttpChannelOverHTTP3 channel)
    {
        super(channel);
    }

    @Override
    protected void reset()
    {
        super.reset();
        contentSource = null;
    }

    @Override
    protected void dispose()
    {
        super.dispose();
        contentSource = null;
    }

    @Override
    protected Content.Source newContentSource()
    {
        contentSource = new ContentSource();
        return contentSource;
    }

    @Override
    public void receive()
    {
        onDataAvailable(null);
    }

    private class ContentSource implements Content.Source
    {
        private static final Logger LOG = LoggerFactory.getLogger(ContentSource.class);

        private Content.Chunk currentChunk;
        private Runnable demandCallback;
        private boolean responseSucceeded;

        @Override
        public Content.Chunk read()
        {
            if (LOG.isDebugEnabled())
                LOG.debug("Reading");
            Content.Chunk chunk = consumeCurrentChunk();
            if (chunk != null)
                return chunk;
            currentChunk = read(false);
            return consumeCurrentChunk();
        }

        private Content.Chunk read(boolean fillInterestIfNeeded)
        {
            if (LOG.isDebugEnabled())
                LOG.debug("Reading, fillInterestIfNeeded={}", fillInterestIfNeeded);
            Stream stream = getHttpChannel().getStream();
            Stream.Data data = stream.readData();
            if (LOG.isDebugEnabled())
                LOG.debug("Read stream data {}", data);
            if (data == null)
            {
                if (fillInterestIfNeeded)
                    stream.demand();
                return null;
            }
            if (data.isLast() && !data.getByteBuffer().hasRemaining())
            {
                data.release();
                return Content.Chunk.EOF;
            }
            return Content.Chunk.from(data.getByteBuffer(), false, data);
        }

        public void onDataAvailable()
        {
            if (LOG.isDebugEnabled())
                LOG.debug("onDataAvailable, demandCallback: {}", demandCallback);
            if (demandCallback != null)
                invokeDemandCallback();
        }

        private Content.Chunk consumeCurrentChunk()
        {
            if (LOG.isDebugEnabled())
                LOG.debug("Consuming current chunk {}; end of stream reached? {}", currentChunk, responseSucceeded);
            if (currentChunk == Content.Chunk.EOF && !responseSucceeded)
            {
                responseSucceeded = true;
                responseSuccess(getHttpExchange());
            }
            Content.Chunk chunk = currentChunk;
            currentChunk = Content.Chunk.next(chunk);
            return chunk;
        }

        @Override
        public void demand(Runnable demandCallback)
        {
            if (LOG.isDebugEnabled())
                LOG.debug("Registering demand {}", demandCallback);
            if (demandCallback == null)
                throw new IllegalArgumentException();
            if (this.demandCallback != null)
                throw new IllegalStateException();
            this.demandCallback = demandCallback;
            meetDemand();
        }

        private void meetDemand()
        {
            if (LOG.isDebugEnabled())
                LOG.debug("Trying to meet demand, current chunk: {}", currentChunk);
            while (true)
            {
                if (currentChunk != null)
                {
                    invokeDemandCallback();
                    break;
                }
                else
                {
                    currentChunk = read(true);
                    if (currentChunk == null)
                        return;
                }
            }
        }

        private void invokeDemandCallback()
        {
            Runnable demandCallback = this.demandCallback;
            this.demandCallback = null;
            if (LOG.isDebugEnabled())
                LOG.debug("Invoking demand callback {}", demandCallback);
            if (demandCallback != null)
            {
                try
                {
                    demandCallback.run();
                }
                catch (Throwable x)
                {
                    fail(x);
                }
            }
        }

        @Override
        public void fail(Throwable failure)
        {
            if (currentChunk != null)
            {
                currentChunk.release();
                failAndClose(failure);
            }
            currentChunk = Content.Chunk.from(failure);
        }
    }

    @Override
    protected HttpChannelOverHTTP3 getHttpChannel()
    {
        return (HttpChannelOverHTTP3)super.getHttpChannel();
    }

    @Override
    public void onResponse(Stream.Client stream, HeadersFrame frame)
    {
        HttpExchange exchange = getHttpExchange();
        if (exchange == null)
            return;

        HttpResponse httpResponse = exchange.getResponse();
        MetaData.Response response = (MetaData.Response)frame.getMetaData();
        httpResponse.version(response.getHttpVersion()).status(response.getStatus()).reason(response.getReason());

        responseBegin(exchange);
        HttpFields headers = response.getFields();
        for (HttpField header : headers)
        {
            responseHeader(exchange, header);
        }

        // TODO: add support for HttpMethod.CONNECT.

        responseHeaders(exchange);
    }

    @Override
    public void onDataAvailable(Stream.Client stream)
    {
        if (LOG.isDebugEnabled())
            LOG.debug("onDataAvailable");

        HttpExchange exchange = getHttpExchange();
        if (exchange == null)
            return;

        if (contentSource != null)
            contentSource.onDataAvailable();
    }

    @Override
    public void onTrailer(Stream.Client stream, HeadersFrame frame)
    {
        HttpExchange exchange = getHttpExchange();
        if (exchange == null)
            return;

        HttpFields trailers = frame.getMetaData().getFields();
        trailers.forEach(exchange.getResponse()::trailer);

        responseSuccess(exchange);
    }

    @Override
    public void onIdleTimeout(Stream.Client stream, Throwable failure, Promise<Boolean> promise)
    {
        HttpExchange exchange = getHttpExchange();
        if (exchange != null)
            exchange.abort(failure, Promise.from(aborted -> promise.succeeded(!aborted), promise::failed));
        else
            promise.succeeded(false);
    }

    @Override
    public void onFailure(Stream.Client stream, long error, Throwable failure)
    {
        responseFailure(failure, Promise.noop());
    }

    private void failAndClose(Throwable failure)
    {
        responseFailure(failure, Promise.from(failed -> getHttpChannel().getHttpConnection().close(failure),
                x -> getHttpChannel().getHttpConnection().close(failure)));
    }
}
