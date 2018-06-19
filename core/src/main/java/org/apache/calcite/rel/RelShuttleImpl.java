/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel;

import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Basic implementation of {@link RelShuttle} that calls
 * {@link RelNode#accept(RelShuttle)} on each child, and
 * {@link RelNode#copy(org.apache.calcite.plan.RelTraitSet, java.util.List)} if
 * any children change.
 */
public class RelShuttleImpl implements RelShuttle {

  protected final Deque<List<RelNode>> childrenStack = new ArrayDeque<>();

  protected final Deque<Boolean> proceedFlagStack = new ArrayDeque<>();

  public boolean doVisit(LogicalAggregate aggregate) {
    return true;
  }

  public boolean doVisit(LogicalMatch match) {
    return true;
  }

  public boolean doVisit(TableScan scan) {
    return true;
  }

  public boolean doVisit(TableFunctionScan scan) {
    return true;
  }

  public boolean doVisit(LogicalValues values) {
    return true;
  }

  public boolean doVisit(LogicalFilter filter) {
    return true;
  }

  public boolean doVisit(LogicalProject project) {
    return true;
  }

  public boolean doVisit(LogicalJoin join) {
    return true;
  }

  public boolean doVisit(LogicalCorrelate correlate) {
    return true;
  }

  public boolean doVisit(LogicalUnion union) {
    return true;
  }

  public boolean doVisit(LogicalIntersect intersect) {
    return true;
  }

  public boolean doVisit(LogicalMinus minus) {
    return true;
  }

  public boolean doVisit(LogicalSort sort) {
    return true;
  }

  public boolean doVisit(LogicalExchange exchange) {
    return true;
  }


  public boolean visit(RelNode other) {
    boolean proceed = switchVisit(other);
    childrenStack.push(new ArrayList<>());
    proceedFlagStack.push(proceed);
    return proceed;
  }

  protected boolean switchVisit(RelNode other) {
    if (other instanceof LogicalAggregate) {
      return doVisit((LogicalAggregate) other);
    }
    if (other instanceof LogicalMatch) {
      return doVisit((LogicalMatch) other);
    }
    if (other instanceof TableScan) {
      return doVisit((TableScan) other);
    }
    if (other instanceof TableFunctionScan) {
      return doVisit((TableFunctionScan) other);
    }
    if (other instanceof LogicalValues) {
      return doVisit((LogicalValues) other);
    }
    if (other instanceof LogicalFilter) {
      return doVisit((LogicalFilter) other);
    }
    if (other instanceof LogicalProject) {
      return doVisit((LogicalProject) other);
    }
    if (other instanceof LogicalJoin) {
      return doVisit((LogicalJoin) other);
    }
    if (other instanceof LogicalCorrelate) {
      return doVisit((LogicalCorrelate) other);
    }
    if (other instanceof LogicalUnion) {
      return doVisit((LogicalUnion) other);
    }
    if (other instanceof LogicalIntersect) {
      return doVisit((LogicalIntersect) other);
    }
    if (other instanceof LogicalMinus) {
      return doVisit((LogicalMinus) other);
    }
    if (other instanceof LogicalSort) {
      return doVisit((LogicalSort) other);
    }
    if (other instanceof LogicalExchange) {
      return doVisit((LogicalExchange) other);
    }
    // TODO support unsupported node types
    return true;
  }

  @Override public RelNode leave(RelNode other) {
    List<RelNode> children = childrenStack.pop();
    boolean proceeded = proceedFlagStack.pop();
    RelNode next;
    if (!proceeded || other instanceof TableScan || other instanceof LogicalValues) {
      next = other;
    } else {
      next = other.copy(other.getTraitSet(), children);
    }
    next = switchLeave(next);
    if (!childrenStack.isEmpty()) {
      childrenStack.peek().add(next);
    }
    return next;
  }

  protected RelNode switchLeave(RelNode other) {
    if (other instanceof LogicalAggregate) {
      return doLeave((LogicalAggregate) other);
    }
    if (other instanceof LogicalMatch) {
      return doLeave((LogicalMatch) other);
    }
    if (other instanceof TableScan) {
      return doLeave((TableScan) other);
    }
    if (other instanceof TableFunctionScan) {
      return doLeave((TableFunctionScan) other);
    }
    if (other instanceof LogicalValues) {
      return doLeave((LogicalValues) other);
    }
    if (other instanceof LogicalFilter) {
      return doLeave((LogicalFilter) other);
    }
    if (other instanceof LogicalProject) {
      return doLeave((LogicalProject) other);
    }
    if (other instanceof LogicalJoin) {
      return doLeave((LogicalJoin) other);
    }
    if (other instanceof LogicalCorrelate) {
      return doLeave((LogicalCorrelate) other);
    }
    if (other instanceof LogicalUnion) {
      return doLeave((LogicalUnion) other);
    }
    if (other instanceof LogicalIntersect) {
      return doLeave((LogicalIntersect) other);
    }
    if (other instanceof LogicalMinus) {
      return doLeave((LogicalMinus) other);
    }
    if (other instanceof LogicalSort) {
      return doLeave((LogicalSort) other);
    }
    if (other instanceof LogicalExchange) {
      return doLeave((LogicalExchange) other);
    }
    // TODO support unsupported node types
    return other;
  }

  public RelNode doLeave(LogicalAggregate aggregate) {
    return aggregate;
  }

  public RelNode doLeave(LogicalMatch match) {
    return match;
  }

  public RelNode doLeave(TableScan scan) {
    return scan;
  }

  public RelNode doLeave(TableFunctionScan scan) {
    return scan;
  }

  public RelNode doLeave(LogicalValues values) {
    return values;
  }

  public RelNode doLeave(LogicalFilter filter) {
    return filter;
  }

  public RelNode doLeave(LogicalProject project) {
    return project;
  }

  public RelNode doLeave(LogicalJoin join) {
    return join;
  }

  public RelNode doLeave(LogicalCorrelate correlate) {
    return correlate;
  }

  public RelNode doLeave(LogicalUnion union) {
    return union;
  }

  public RelNode doLeave(LogicalIntersect intersect) {
    return intersect;
  }

  public RelNode doLeave(LogicalMinus minus) {
    return minus;
  }

  public RelNode doLeave(LogicalSort sort) {
    return sort;
  }

  public RelNode doLeave(LogicalExchange exchange) {
    return exchange;
  }

}

// End RelShuttleImpl.java
