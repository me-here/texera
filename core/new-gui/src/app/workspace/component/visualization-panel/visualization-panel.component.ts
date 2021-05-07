import {Component, Input, OnChanges, OnInit} from '@angular/core';
import {WorkflowStatusService} from '../../service/workflow-status/workflow-status.service';
import {ExecutionState, IncrementalOutputResult} from '../../types/execute-workflow.interface';
import {ChartType, WordCloudTuple} from '../../types/visualization.interface';
import * as cloud from 'd3-cloud';
import * as d3 from 'd3';
import * as c3 from 'c3';
import {Primitive, PrimitiveArray} from 'c3';
import * as mapboxgl from 'mapbox-gl';
import {ScatterplotLayerProps} from '@deck.gl/layers/scatterplot-layer/scatterplot-layer';
import {MapboxLayer} from '@deck.gl/mapbox';
import {ScatterplotLayer} from '@deck.gl/layers';
import {ExecuteWorkflowService} from '../../service/execute-workflow/execute-workflow.service';

/**
 * VisualizationPanelComponent displays the button for visualization in ResultPanel when the result type is chart.
 *
 * It receives the data for visualization and chart type.
 * When user click on button, this component will open VisualizationPanelContentComponent and display figure.
 * User could click close at the button of VisualizationPanelContentComponent to exit the visualization panel.
 * @author Mingji Han
 */
@Component({
  selector: 'texera-visualization-panel',
  templateUrl: './visualization-panel.component.html',
  styleUrls: ['./visualization-panel.component.scss']
})
export class VisualizationPanelComponent implements OnInit, OnChanges {

  // this readonly variable must be the same as HTML element ID for visualization
  public static readonly CHART_ID = '#texera-result-chart-content';
  public static readonly CHART_CONTAINER = 'texera-result-chart-content';
  public static readonly MAP_CONTAINER = 'texera-result-map-container';

  // width and height of the canvas in px
  public static readonly WIDTH = 1000;
  public static readonly HEIGHT = 800;

  @Input()
  operatorID: string | undefined;

  displayVisualizationPanel: boolean = false;
  displayMap: boolean = false;
  data: object[] | undefined;
  chartType: ChartType | undefined;
  columns: string[] = [];
  batchID: number = 0;
  private d3ChartElement: d3.Selection<SVGGElement, unknown, HTMLElement, any> | undefined;
  private c3ChartElement: c3.ChartAPI | undefined;
  private map: mapboxgl.Map | undefined;
  private result: IncrementalOutputResult | undefined;

  constructor(
    private workflowStatusService: WorkflowStatusService,
    private executeWorkflowService: ExecuteWorkflowService,
  ) {
    this.workflowStatusService.getResultUpdateStream().subscribe(event => {
      this.displayMap = false;
      this.displayVisualizationPanel = false;
      if (!this.operatorID) {
        return;
      }
      this.result  = this.workflowStatusService.getCurrentIncrementalResult()[this.operatorID];
      this.updateDisplayVisualizationPanel();
      this.drawChartWithResultSnapshot();
    });
    this.executeWorkflowService.getExecutionStateStream().subscribe(event => {
      if (event.current.state === ExecutionState.WaitingToRun) {
        this.clearPrevResults();
      }
    });
  }

  ngOnInit() {
    this.initMap();
  }
  ngOnChanges() {
    this.updateDisplayVisualizationPanel();
  }

  clearPrevResults() {
    if (this.d3ChartElement) {
      this.d3ChartElement.remove();
    }
    if (this.c3ChartElement) {
      this.c3ChartElement.destroy();
    }
    this.clearAllMapLayers();
  }

  clearAllMapLayers() {
    while (this.batchID > 0) {
      this.map?.removeLayer('scatter' + this.batchID);
      this.batchID--;
    }
  }

  updateDisplayVisualizationPanel() {
    this.displayMap = false;
    this.displayVisualizationPanel = false;
    if (!this.operatorID) {
      return;
    }


    if (this.result?.result.chartType !== undefined) {
      if (this.result?.result.chartType === 'spatial scatterplot') {
        this.displayMap = true;
      } else {
        this.displayVisualizationPanel = true;
      }
    }
  }

  drawChartWithResultSnapshot() {

    if (!this.result) {
      return;
    }

    this.data = this.result.result.table[1] as object[];
    this.chartType = this.result.result.chartType;
    if (!this.data || !this.chartType) {
      return;
    }
    if (this.data?.length < 1) {
      return;
    }
    switch (this.chartType) {
      // correspond to WordCloudSink.java
      case ChartType.WORD_CLOUD:
        this.generateWordCloud();
        break;
      // correspond to TexeraBarChart.java
      case ChartType.BAR:
      case ChartType.STACKED_BAR:
      // correspond to PieChartSink.java
      case ChartType.PIE:
      case ChartType.DONUT:
      // correspond to TexeraLineChart.java
      case ChartType.LINE:
      case ChartType.SPLINE:
        this.generateChart();
        break;
      case ChartType.SCATTERPLOT:
        this.simpleScatterplot();
        break;
      case ChartType.SPATIAL_SCATTERPLOT:
        this.spatialScatterplot();
        break;
    }
  }

  generateWordCloud() {
    if (!this.data || !this.chartType) {
      return;
    }

    if (this.d3ChartElement === undefined) {
      this.d3ChartElement = this.createD3Container();
    }
    if (this.data?.length < 1) {
      return;
    }
    const wordCloudTuples = this.data as ReadonlyArray<WordCloudTuple>;

    const drawWordCloud = (words: cloud.Word[]) => {
      if (!this.d3ChartElement) {
        return;
      }
      const d3Fill = d3.scaleOrdinal(d3.schemeCategory10);

      const wordCloudData = this.d3ChartElement.selectAll<d3.BaseType, cloud.Word>('g text').data(words, d => d.text ?? '');

      wordCloudData.enter()
        .append('text')
        .style('font-size', (d) => d.size ?? 0 + 'px')
        .style('fill', d => d3Fill(d.text ?? ''))
        .attr('font-family', 'Impact')
        .attr('text-anchor', 'middle')
        .attr('transform', (d) => 'translate(' + [d.x, d.y] + ')rotate(' + d.rotate + ')')
        // this text() call must be at the end or it won't work
        .text(d => d.text ?? '');

      // Entering and existing words
      wordCloudData.transition()
        .duration(600)
        .attr('font-family', 'Impact')
        .style('font-size', d => d.size + 'px')
        .attr('transform', d => 'translate(' + [d.x, d.y] + ')rotate(' + d.rotate + ')')
        .style('fill-opacity', 1);

      // Exiting words
      wordCloudData.exit()
        .transition()
        .duration(200)
        .attr('font-family', 'Impact')
        .style('fill-opacity', 1e-6)
        .attr('font-size', 1)
        .remove();
    };

    const minCount = Math.min(...wordCloudTuples.map(t => t.count));
    const maxCount = Math.max(...wordCloudTuples.map(t => t.count));

    const minFontSize = 50;
    const maxFontSize = 150;

    const d3Scale = d3.scaleLinear();
    // const d3Scale = d3.scaleSqrt();
    // const d3Scale = d3.scaleLog();

    d3Scale.domain([minCount, maxCount]).range([minFontSize, maxFontSize]);

    const layout = cloud()
      .size([VisualizationPanelComponent.WIDTH, VisualizationPanelComponent.HEIGHT])
      .words(wordCloudTuples.map(t => ({ text: t.word, size: d3Scale(t.count) })))
      .text(d => d.text ?? '')
      .padding(5)
      .rotate(() => 0)
      .font('Impact')
      .fontSize(d => d.size ?? 0)
      .random(() => 1)
      .on('end', drawWordCloud);

    layout.start();
  }

  generateChart() {
    if (!this.data || !this.chartType) {
      return;
    }
    if (this.data?.length < 1) {
      return;
    }
    const dataToDisplay: Array<[string, ...PrimitiveArray]> = [];
    const category: string[] = [];
    for (let i = 1; i < this.columns?.length; i++) {
      category.push(this.columns[i]);
    }

    const columnCount = this.columns.length;

    for (const row of this.data) {
      const items: [string, ...PrimitiveArray] = [Object.values(row)[0]];
      for (let i = 1; i < columnCount; i++) {
        items.push(Number((Object.values(row)[i])));
      }
      dataToDisplay.push(items);
    }

    if (this.c3ChartElement) {
      this.c3ChartElement.destroy();
    }
    this.c3ChartElement = c3.generate({
      size: {
        height: VisualizationPanelComponent.HEIGHT,
        width: VisualizationPanelComponent.WIDTH
      },
      data: {
        columns: dataToDisplay,
        type: this.chartType as c3.ChartType
      },
      axis: {
        x: {
          type: 'category',
          categories: category
        }
      },
      bindto: VisualizationPanelComponent.CHART_ID
    });

  }

  simpleScatterplot() {
    if (this.c3ChartElement !== undefined) {
      this.updateSimpleScatterPlot();
    } else {
      this.initSimpleScatterPlot();
    }
  }
  updateSimpleScatterPlot() {
    const result = this.data as Array<Record<string, Primitive>>;
    const xLabel: string = Object.keys(result[0])[0];
    const yLabel: string = Object.keys(result[0])[1];
    this.c3ChartElement?.load({
      json: result,
      keys: {
        x: xLabel,
        value: [yLabel]
      }
    });
  }
  initSimpleScatterPlot() {
    const result = this.data as Array<Record<string, Primitive>>;
    const xLabel: string = Object.keys(result[0])[0];
    const yLabel: string = Object.keys(result[0])[1];

    this.c3ChartElement = c3.generate({
      size: {
        height: VisualizationPanelComponent.HEIGHT,
        width: VisualizationPanelComponent.WIDTH
      },
      data: {
        json: result,
        keys: {
          x: xLabel,
          value: [yLabel]
        },
        type: this.chartType as c3.ChartType
      },
      axis: {
        x: {
          label: xLabel,
          tick: {
            fit: true
          }
        },
        y: {
          label: yLabel
        }
      },
      bindto: VisualizationPanelComponent.CHART_ID
    });
  }

  spatialScatterplot() {
      this.addScatterLayer();
  }

  initMap() {
    if (this.map === undefined) {
      /* mapbox object with default configuration */
      this.map = new mapboxgl.Map({
        container: VisualizationPanelComponent.MAP_CONTAINER,
        style: 'mapbox://styles/mapbox/light-v9',
        center: [-96.35, 39.5],
        zoom: 3,
        maxZoom: 17,
        minZoom: 0
      });
    }
  }

  addScatterLayer() {
    if (this.map !== undefined) {
      this.batchID++;
      const props: ScatterplotLayerProps<any> = {
        opacity: 0.8,
        filled: true,
        radiusScale: 100,
        radiusMinPixels: 1,
        radiusMaxPixels: 25,
        getPosition: (d: { longitude: any; latitude: any; }) => [d.longitude, d.latitude],
        getFillColor: [57, 73, 171]
      };

      const clusterLayer = new MapboxLayer({
        id: 'scatter' + this.batchID,
        type: ScatterplotLayer,
        data: this.data,
        pickable: true,
      });
      clusterLayer.setProps(props);
      this.map.addLayer(clusterLayer);
    }
  }

  createD3Container() {
    return d3.select(`#${VisualizationPanelComponent.CHART_CONTAINER}`)
      .append('svg')
      .attr('width', VisualizationPanelComponent.WIDTH)
      .attr('height', VisualizationPanelComponent.HEIGHT)
      .append('g')
      .attr('transform',
        'translate(' + VisualizationPanelComponent.WIDTH / 2 + ','
        + VisualizationPanelComponent.HEIGHT / 2 + ')');
  }
}
