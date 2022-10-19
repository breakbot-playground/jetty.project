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

package org.eclipse.jetty.fcgi.client.http;

import org.eclipse.jetty.client.HttpChannel;
import org.eclipse.jetty.client.HttpExchange;
import org.eclipse.jetty.client.HttpReceiver;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.io.Content;
import org.eclipse.jetty.util.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpReceiverOverFCGI extends HttpReceiver
{
    private static final Logger LOG = LoggerFactory.getLogger(HttpReceiverOverFCGI.class);

    private ContentSource contentSource;
    private Content.Chunk chunk;

    public HttpReceiverOverFCGI(HttpChannel channel)
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

    public void receive()
    {
        if (contentSource == null)
        {
            HttpConnectionOverFCGI httpConnection = getHttpChannel().getHttpConnection();
            boolean setFillInterest = httpConnection.parseAndFill();
            if (contentSource == null && setFillInterest)
                httpConnection.fillInterested();
        }
        else
        {
            contentSource.onDataAvailable();
        }
    }

    void content(Content.Chunk chunk)
    {
        if (this.chunk != null)
            throw new IllegalStateException();
        this.chunk = chunk;
        contentSource.onDataAvailable();
    }

    void end(HttpExchange exchange)
    {
        if (chunk != null)
            throw new IllegalStateException();
        chunk = Content.Chunk.EOF;
        responseSuccess(exchange, getHttpChannel().getHttpConnection()::fillInterested);
    }

    @Override
    protected Content.Source newContentSource()
    {
        contentSource = new ContentSource();
        return contentSource;
    }

    private class ContentSource implements Content.Source
    {
        private Content.Chunk currentChunk;
        private Runnable demandCallback;

        @Override
        public Content.Chunk read()
        {
            if (LOG.isDebugEnabled())
                LOG.debug("read");
            Content.Chunk chunk = consumeCurrentChunk();
            if (chunk != null)
                return chunk;
            currentChunk = HttpReceiverOverFCGI.this.read(false);
            return consumeCurrentChunk();
        }

        public void onDataAvailable()
        {
            if (LOG.isDebugEnabled())
                LOG.debug("onDataAvailable");
            if (demandCallback != null)
                invokeDemandCallback();
        }

        private Content.Chunk consumeCurrentChunk()
        {
            if (LOG.isDebugEnabled())
                LOG.debug("consumeCurrentChunk");
            Content.Chunk chunk = currentChunk;
            currentChunk = Content.Chunk.next(chunk);
            return chunk;
        }

        @Override
        public void demand(Runnable demandCallback)
        {
            if (LOG.isDebugEnabled())
                LOG.debug("demand");
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
                LOG.debug("meetDemand");
            while (true)
            {
                if (currentChunk != null)
                {
                    invokeDemandCallback();
                    break;
                }
                else
                {
                    currentChunk = HttpReceiverOverFCGI.this.read(true);
                    if (currentChunk == null)
                        return;
                }
            }
        }

        private void invokeDemandCallback()
        {
            if (LOG.isDebugEnabled())
                LOG.debug("invokeDemandCallback");
            Runnable demandCallback = this.demandCallback;
            this.demandCallback = null;
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

    private Content.Chunk read(boolean fillInterestIfNeeded)
    {
        Content.Chunk chunk = consumeChunk();
        if (chunk != null)
            return chunk;
        HttpConnectionOverFCGI httpConnection = getHttpChannel().getHttpConnection();
        boolean needFillInterest = httpConnection.parseAndFill();
        chunk = consumeChunk();
        if (chunk != null)
            return chunk;
        if (needFillInterest && fillInterestIfNeeded)
            httpConnection.fillInterested();
        return null;
    }

    private Content.Chunk consumeChunk()
    {
        Content.Chunk chunk = this.chunk;
        this.chunk = null;
        return chunk;
    }

    @Override
    protected HttpChannelOverFCGI getHttpChannel()
    {
        return (HttpChannelOverFCGI)super.getHttpChannel();
    }

    @Override
    protected void responseBegin(HttpExchange exchange)
    {
        super.responseBegin(exchange);
    }

    @Override
    protected void responseHeader(HttpExchange exchange, HttpField field)
    {
        super.responseHeader(exchange, field);
    }

    @Override
    protected void responseHeaders(HttpExchange exchange)
    {
        super.responseHeaders(exchange);
    }

    @Override
    protected void responseFailure(Throwable failure, Promise<Boolean> promise)
    {
        super.responseFailure(failure, promise);
    }

    private void failAndClose(Throwable failure)
    {
        responseFailure(failure, Promise.from(failed ->
        {
            if (failed)
                getHttpChannel().getHttpConnection().close(failure);
        }, x -> getHttpChannel().getHttpConnection().close(failure)));
    }
}
