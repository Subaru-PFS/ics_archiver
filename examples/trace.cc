// A ROOT macro for plotting archiver trace data

#include <iostream>
#include <fstream>
#include <string>

TCanvas *elapsed,*rates;
TH2F *frame,*frame2;

TGraph *clientGraph = new TGraph();
TGraph *tableInGraph = new TGraph();
TGraph *tableOutGraph = new TGraph();

TGraph *clientRate;
TGraph *tableInRate;
TGraph *tableOutRate;

TGraph *rateGraph(const TGraph *inGraph) {
	int index,npt = inGraph->GetN() - 1;
	outGraph = new TGraph(2*npt);
	double dt1,dt2,n1,n2;
	for(index = 0; index < npt; index++) {
		inGraph->GetPoint(index,dt1,n1);
		inGraph->GetPoint(index+1,dt2,n2);
		double rate = 1e-3*(n2-n1)/(dt2-dt1);
		if(rate <= 0) rate = 1e-6;
		outGraph->SetPoint(2*index,dt1,rate);
		outGraph->SetPoint(2*index+1,dt2,rate);
	}
	return outGraph;
}

void trace(const char *tableFile="reply_hdr.trace", const char *clientFile="timing.dat") {
	ifstream *table = new ifstream(tableFile);
	string tag;
	int n,nMax;
	double tableStart,clientStart,dt,dtMax = 0;
	*table >> tag >> tableStart;
	while(table->good()) {
		*table >> tag >> n >> dt;
		if(n > nMax) nMax = n;
		if(dt > dtMax) dtMax = dt;
		if(table->eof()) break;
		if(tag == "IN") {
			tableInGraph->SetPoint(tableInGraph->GetN(),dt,(double)n);
		}
		else if(tag == "OUT") {
			tableOutGraph->SetPoint(tableOutGraph->GetN(),dt,(double)n);
		}
	}
	table->close();
	delete table;
	
	ifstream *client = new ifstream(clientFile);
	*client >> tag >> clientStart;
	while(client->good()) {
		*client >> n >> dt;
		dt += clientStart - tableStart;
		clientGraph->SetPoint(clientGraph->GetN(),dt,(double)n);
	}
	client->close();
	delete client;

	clientRate = rateGraph(clientGraph);
	tableInRate = rateGraph(tableInGraph);
	tableOutRate = rateGraph(tableOutGraph);

	gStyle->SetOptStat(0);
	gStyle->SetOptTitle(0);
	
	elapsed = new TCanvas("elapsed","elapsed",800,600);
	frame = new TH2F("frame","frame",1,-0.05*dtMax,1.05*dtMax,1,0.,1.05*nMax);
	frame->SetXTitle("Elapsed Time (secs)");
	frame->SetYTitle("Messages Handled");
	frame->Draw();
	tableInGraph->Draw("LSAME");
	tableInGraph->SetLineColor(kBlue);
	tableOutGraph->Draw("LSAME");
	tableOutGraph->SetLineColor(kRed);
	clientGraph->Draw("LSAME");
	clientGraph->SetLineColor(kGreen);

	rates = new TCanvas("rates","rates",800,600);
	rates->SetLogy();
	frame2 = new TH2F("frame2","frame2",1,-0.05*dtMax,1.05*dtMax,1,1e-2,1e2);
	frame2->SetXTitle("Elapsed Time (secs)");
	frame2->SetYTitle("Message Handling Rate (kHz)");
	frame2->Draw();
	tableInRate->Draw("LSAME");
	tableInRate->SetLineColor(kBlue);
	tableOutRate->Draw("LSAME");
	tableOutRate->SetLineColor(kRed);
	clientRate->Draw("LSAME");
	clientRate->SetLineColor(kGreen);
}