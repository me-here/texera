import { Component, Input, OnInit, AfterViewInit, OnDestroy } from '@angular/core';
import * as c3 from 'c3';
import { PrimitiveArray } from 'c3';
import { ChartType, WordCloudTuple, DialogData } from '../../types/visualization.interface';
import * as d3 from 'd3';
import * as cloud from 'd3-cloud';
import { WorkflowStatusService } from '../../service/workflow-status/workflow-status.service';
import { Subscription } from 'rxjs';


/**
 * VisualizationPanelContentComponent displays the chart based on the chart type and data in table.
 *
 * It will convert the table into data format required by c3.js.
 * Then it passes the data and figure type to c3.js for rendering the figure.
 * @author Mingji Han, Xiaozhen Liu
 */
@Component({
  selector: 'texera-visualization-panel-content',
  templateUrl: './visualization-panel-content.component.html',
  styleUrls: ['./visualization-panel-content.component.scss']
})
export class VisualizationPanelContentComponent implements OnInit, AfterViewInit, OnDestroy {
  // this readonly variable must be the same as HTML element ID for visualization
  public static readonly CHART_ID = '#texera-result-chart-content';
  public static readonly WORD_CLOUD_ID = 'texera-word-cloud';
  public static readonly WIDTH = 1000;
  public static readonly HEIGHT = 800;

  @Input()
  public data: DialogData | undefined;
  @Input()
  public operatorID: string | undefined;

  private columns: string[] = [];

  private subscription: Subscription | undefined;

  constructor(
    private workflowStatusService: WorkflowStatusService
  ) {
  }

  ngOnInit() {
    if (! this.data) {
      return;
    }
    this.columns = Object.keys(this.data.table[0]).filter(x => x !== '_id');
  }

  ngAfterViewInit() {
    if (! this.data) {
      return;
    }
    switch (this.data.chartType) {
      // correspond to WordCloudSink.java
      case ChartType.WORD_CLOUD: this.onClickGenerateWordCloud(); break;
      // correspond to TexeraBarChart.java
      case ChartType.BAR:
      case ChartType.STACKED_BAR:
      // correspond to PieChartSink.java
      case ChartType.PIE:
      case ChartType.DONUT:
      // correspond to TexeraLineChart.java
      case ChartType.LINE:
      case ChartType.SPLINE: this.onClickGenerateChart(); break;
    }

    this.subscription = this.workflowStatusService.getStatusUpdateStream().subscribe(update => {
      if (! this.operatorID) {
        return;
      }
      const operatorUpdate = update[this.operatorID];
      if (! operatorUpdate || ! operatorUpdate.aggregatedOutputResults) {
        return;
      }
      const table = operatorUpdate.aggregatedOutputResults.table;
      const chartType = operatorUpdate.aggregatedOutputResults.chartType;
      if (! chartType) {
        return;
      }
      this.data = {
        table:  table as any,
        chartType:  chartType
      };
      this.onClickGenerateChart();
    });

  }

  ngOnDestroy() {
    if (this.subscription) {
      this.subscription.unsubscribe();
    }
  }

  onClickGenerateWordCloud() {
    if (! this.data) {
      return;
    }
    const dataToDisplay: object[] = [];
    const wordCloudTuples = this.data.table as ReadonlyArray<WordCloudTuple>;

    for (const tuple of wordCloudTuples) {
      dataToDisplay.push([tuple.word, tuple.size]);
    }

    const wordCloudElement =
    d3.select(`#${VisualizationPanelContentComponent.WORD_CLOUD_ID}`)
      .append('svg')
      .attr('width', VisualizationPanelContentComponent.WIDTH)
      .attr('height', VisualizationPanelContentComponent.HEIGHT)
      .append('g')
        .attr('transform', 'translate(' + 500 / 2 + ',' + 500 / 2 + ')');

    const drawWordCloud = (words: cloud.Word[]) => {

      const wordCloudData = wordCloudElement.selectAll('g text').data(words);

      wordCloudData.enter()
        .append('text')
        .style('font-size', (d) => d.size ?? 0 + 'px')
        .style('font-family', 'Impact')
        .attr('text-anchor', 'middle')
        .attr('transform', (d) => 'translate(' + [d.x, d.y] + ')rotate(' + d.rotate + ')')
        .text((d: any) => d.text);

      // Entering and existing words
      wordCloudData.transition()
        .duration(600)
        .style('font-size', d => d.size + 'px')
        .attr('transform', (d) => 'translate(' + [d.x, d.y] + ')rotate(' + d.rotate + ')')
        .style('fill-opacity', 1);

    // Exiting words
    wordCloudData.exit()
        .transition()
            .duration(100)
            .style('fill-opacity', 1e-6)
            .attr('font-size', 1)
            .remove();
    };

    console.log(wordCloudTuples);
    const layout = cloud()
      .size([VisualizationPanelContentComponent.WIDTH, VisualizationPanelContentComponent.HEIGHT])
      .words(wordCloudTuples.map(t => ({text: t.word, size: t.size})))
      // .words([
      //   'Hello', 'world', 'normally', 'you', 'want', 'more', 'words',
      //   'than', 'this'].map(function(d) {
      //   return {text: d, size: 10 + Math.random() * 90, test: 'haha'};
      // }))
      .padding(5)
      .rotate(() => 0)
      .font('Impact')
      .fontSize(d => d.size ?? 0)
      .on('end', drawWordCloud);

    layout.start();

  }

  onClickGenerateChart() {
    if (! this.data) {
      return;
    }

    const dataToDisplay: Array<[string, ...PrimitiveArray]> = [];
    const category: string[] = [];
    for (let i = 1; i < this.columns?.length; i++) {
      category.push(this.columns[i]);
    }

    const columnCount = this.columns.length;

    for (const row of this.data.table) {
      const items: [string, ...PrimitiveArray] = [Object.values(row)[0]];
      for (let i = 1; i < columnCount; i++) {
        items.push(Number((Object.values(row)[i])));
      }
      dataToDisplay.push(items);
    }

    c3.generate({
      size: {
        height: VisualizationPanelContentComponent.HEIGHT,
        width: VisualizationPanelContentComponent.WIDTH
      },
      data: {
        columns: dataToDisplay,
        type: this.data.chartType as c3.ChartType
      },
      axis: {
        x: {
          type: 'category',
          categories: category
        }
      },
      bindto: VisualizationPanelContentComponent.CHART_ID
    });
  }

}
