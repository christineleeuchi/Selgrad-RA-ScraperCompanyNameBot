import contextlib
import itertools
import re
import sys
import tomllib
from copy import deepcopy
from itertools import islice
from pathlib import Path
from string import ascii_uppercase, digits

import pandas
from addict import Dict
from pdfminer.high_level import extract_pages
from pdfminer.layout import (LTChar, LTFigure, LTTextBoxHorizontal,
                             LTTextLineHorizontal)
from pdfminer.pdfparser import PDFSyntaxError
from tqdm import tqdm


class ReportReader:
    fp = None
    d15, d16 = None, None
    skip = False
    nocaching = False
    skip_step3 = False
    skip_step4 = False
    skip_step5 = False
    skip_report = False
    version = None
    pages = Dict(cover=None, body=[], summary=[], report=[])
    data = Dict(
        company_name=None,
        company_abbr=None,
        fiscal_year=None,
        report_date=None,
        summary=[],
        report=[],
        error=set(),
    )
    cache = Dict(
        summary=Dict(
            columns={},
            column_titles=[],
            levels={},
            topics={},
            issuance_dates={},
            combined_items={},
            rows={},
        ),
        report=Dict(
            period_reports={},
            headers={},
            issue_dates={},
            contents={},
            updated_contents={},
            levels={},
            topics={},
            report_items={},
            sections={},
            updated_sections={},
            guid_header={},
            guid_sections={},
            sectioned_report=[],
        ),
    )

    def __init__(self, fp: Path | str, nocaching=False, cp=None):
        self.fp = Path(fp)
        if not self.fp.exists():
            raise FileNotFoundError("File not found")
        self.nocaching = nocaching
        if cp is None:
            cp = Path(__file__).parent / "config.toml"
        if not cp.exists():
            raise FileNotFoundError("Config file not found")
        with open(cp, "rb") as cf:
            c = tomllib.load(cf)
        for key, value in c.items():
            setattr(self, key, value)

    def read(self):
        self.start()
        if not self.skip:
            self.set_params()
            self.step1()
            self.step2()
            if not self.skip_step3:
                self.step3()
            if not self.skip_step4:
                self.step4()
            if not self.skip_step5:
                self.step5()
            if not self.skip_report:
                self.report_step1()
                self.report_step2()
                self.report_step3()
                self.report_step4()
                self.report_step5()
                self.report_step6()
                self.report_step7()
                self.report_step8()
            if self.nocaching:
                self.cache = Dict()

    @property
    def summary(self):
        return pandas.DataFrame(self.data.summary) if self.data.summary else None

    @property
    def report(self):
        return pandas.DataFrame(self.data.report) if self.data.report else None

    @property
    def error(self):
        return [{"REPORT_NAME": self.fp.name, "CHECK": e} for e in self.data.error]

    def start(self):
        """
        Starts the PDF extraction process.

        Returns:
            self: The current instance of the Reader class.
        """

        try:
            pages = list(extract_pages(self.fp))
        except OSError:
            self.data.error.add("File cannot be read")
            self.skip = True
            return self
        except PDFSyntaxError:
            self.data.error.add("File is not a valid PDF")
            self.skip = True
            return self
        cover, *body = islice(pages, None, len(pages) - 1)
        self.pages.cover = cover
        self.pages.body = body
        return self

    def set_params(self):
        """
        Sets the parameters for the financial report extraction.

        This method determines the version of the report and sets the corresponding parameters
        based on the content of the cover page.

        Returns:
            self: The instance of the class with updated parameters.

        Raises:
            ValueError: If the report version is not supported.
        """

        fbs = [b for b in self.pages.cover if isinstance(b, LTFigure)]
        bs = list(self.pages.cover)
        if bs[0].get_text().strip() == self.s1:
            self.version = 1
            self.d15 = 742
            self.d16 = 66.995
        elif (715.8, 594) in [(b.y0, b.x1) for b in fbs]:
            self.version = 2
            self.d15 = 758
            self.d16 = 76.880
        elif (677.012, 977.88) in [(b.y0, b.x1) for b in fbs]:
            self.version = 3
            self.d15 = 758.310
            self.d16 = 76.880
        else:
            raise ValueError("Unsupported report version")
        return self

    def step1(self):
        """
        Extracts information from the cover page of the financial report.

        Returns:
            self: The current instance of the class.
        """

        items = list(self.pages.cover)
        if self.version == 1:
            title = items[2].get_text().replace("\n", " ")
            p = re.compile("((.*) - .*) Fiscal Year (\d{4})", re.MULTILINE)
            if match := p.match(title):
                self.company_name, self.company_abbr, self.fiscal_year = match.groups()[
                    :3
                ]
            pd = re.compile("Report Generated: (.*)")
            if match := pd.match(items[3].get_text()):
                self.report_date = match[1]
        elif self.version in [2, 3]:
            title = items[1].get_text().replace("\n", " ")
            p = re.compile(
                "([^-]*( - (.*))?) / FY (\d{4}) REPORT GENERATED: (.*)", re.MULTILINE
            )
            if match := p.match(title):
                (
                    self.data.company_name,
                    _,
                    self.data.company_abbr,
                    self.data.fiscal_year,
                    self.data.report_date,
                ) = match.groups()
        return self

    def step2(self):
        """
        Extracts the summary and report pages from the PDF based on the report title and version.

        Returns:
            self: The current instance of the class.
        """

        report_title = (
            "Period Report"
            if self.data.company_abbr is None
            else f"{self.data.company_abbr} Period Report"
        )
        if self.version == 1:
            for pn, p in enumerate(self.pages.body):
                if pn == 0:
                    continue
                blocks = self.sorty(
                    [
                        b
                        for b in list(p)
                        if isinstance(b, (LTTextBoxHorizontal, LTTextLineHorizontal))
                    ]
                )

                if report_title in blocks[2].get_text():
                    self.pages.summary = self.pages.body[:pn]
                    self.pages.report = self.pages.body[pn:]
                    break
        elif self.version in [2, 3]:
            for pn, p in enumerate(self.pages.body):
                if pn == 0:
                    continue
                blocks = self.sorty(
                    [
                        b
                        for b in list(p)
                        if isinstance(b, (LTTextBoxHorizontal, LTTextLineHorizontal))
                    ]
                )
                if (
                    isinstance(blocks[1], LTTextBoxHorizontal)
                    and report_title.lower() in blocks[1].get_text().lower()
                ):
                    self.pages.summary = self.pages.body[:pn]
                    self.pages.report = self.pages.body[pn:]
                    break
        psn = len(self.pages.summary)
        prn = len(self.pages.report)
        if psn == 0:
            self.data.error.add("No summary pages found")
            self.skip_step3 = True
        if prn == 0:
            self.data.error.add("No report pages found")
            self.skip_report = True
        return self

    def step3(self):
        """
        Extracts summary columns from the financial report pages.

        Returns:
            self: The current instance of the class.
        """

        columns = {}
        bs = self.get_bs(self.pages.summary[0])
        bs, _ = self.rm_id(bs)
        bs = self.sorty(bs)[2:]
        titles = [{"title": c.get_text().strip(), "right": c.x1} for c in bs[:5]]
        for page_num, page in enumerate(self.pages.summary):
            if page_num not in columns:
                columns[page_num] = {}
            for title in titles:
                bs = [
                    list(block) if block.get_text().count("\n") > 1 else [block]
                    for block in self.get_bs(page)
                    if round(block.x1, 3) == round(title["right"], 3)
                    or round(title["right"] - block.x1, 2) == 3.06
                ]
                bs = self.sorty(list(itertools.chain(*bs)))
                bs = bs[1:]
                columns[page_num][title["title"]] = bs
        self.cache.summary.columns = columns
        self.cache.summary.column_titles = titles
        if not all(
            t["title"].startswith(("Q1", "Q2", "Q3", "Q4", "FY")) for t in titles
        ):
            self.skip_step4 = True
            self.data.error.add("No valid summary columns found")

        return self

    def step4(self):
        """
        Extracts information from the summary pages of a financial report.

        This method processes the summary pages of a financial report and extracts
        relevant information such as headers, sections, levels, and rows. It performs
        various operations on the pages and their content to organize the data
        structure for further analysis.

        Returns:
            self: The current instance of the class with the extracted information
                  stored in the cache attributes.

        Raises:
            None
        """
        LTheaders = {}
        SIdates = {}
        SHeaders = {}
        SSections = {}
        SLevels = {}
        SRows = {}
        psn = len(self.pages.summary)
        for pn, p in enumerate(self.pages.summary):
            bs = [
                list(block) if self.is_mb(block) else [block]
                for block in self.get_bs(p)
            ]
            bs = self.sorty(list(itertools.chain(*bs)))
            (
                bs,
                SIdates[pn],
            ) = self.rm_id(bs)
            SIdates[pn] = self.sorty(SIdates[pn])
            if pn + 1 == psn:
                bs = [
                    b
                    for b in bs
                    if not b.get_text().strip().startswith("*")
                    and not b.get_text().strip().startswith(":")
                ]
            if pn == 0:
                bs = self.sorty(bs)[7:]
            LTheaders[pn] = self.sorty([b for b in bs if b.x0 == self.d01])
            SHeaders[pn] = []
            RHeaders = self.sorty([b for b in bs if b.x0 == self.d02])
            for rhi, rh in enumerate(RHeaders):
                if rhi >= 1:
                    if abs(round(RHeaders[rhi - 1].y0 - rh.y0, 1)) == self.d12:
                        SHeaders[pn][-1] = [
                            SHeaders[pn][-1],
                            rh,
                        ]
                    else:
                        SHeaders[pn].append(rh)
                else:
                    SHeaders[pn].append(rh)

        for pn, pbs in LTheaders.items():
            SSections[pn] = []
            SLevels[pn] = []
            for pb in pbs:
                if self.is_mb(pb):
                    for sb in list(pb):
                        if sb.x0 == self.d01:
                            SSections[pn].append(sb)
                        elif pn in SHeaders:
                            SHeaders[pn].append(sb)
                        else:
                            SHeaders[pn] = [sb]
                else:
                    SSections[pn].append(pb)
            SSections[pn] = self.sorty(SSections[pn])
            for si, s in enumerate(SSections[pn]):
                if si + 1 < len(SSections[pn]) and (
                    round(
                        s.y0 - SSections[pn][si + 1].y0,
                        2,
                    )
                    == 11.46
                ):
                    SLevels[pn].append(s)
            SSections[pn] = [s for s in SSections[pn] if s not in SLevels[pn]]
        LSections = {}
        PLevel = None
        for pn, pss in SSections.items():
            LSections[pn] = []
            if not SLevels[pn]:
                LSections[pn] = [(PLevel, section) for section in pss]
                continue
            SPLevel = [s for s in pss if s.y0 > SLevels[pn][0].y1]
            if SPLevel:
                LSections[pn].extend([(PLevel, section) for section in SPLevel])
            pss = [s for s in pss if s not in SPLevel]
            for li, l in enumerate(SLevels[pn]):
                if li + 1 < len(SLevels[pn]):
                    LSections[pn].extend(
                        [(l, s) for s in pss if SLevels[pn][li + 1].y1 < s.y0 < l.y0]
                    )
                else:
                    LSections[pn].extend(
                        [(l, section) for section in pss if section.y0 < l.y0]
                    )
                PLevel = l
        SEHeaders = {}
        PSection = None
        for pn, phs in SHeaders.items():
            SEHeaders[pn] = []
            if not LSections[pn]:
                SEHeaders[pn] = [PSection + (h,) for h in phs]
                continue
            HPLevel = [h for h in phs if self.get_hy(h, "y0") > LSections[pn][0][1].y1]
            if HPLevel:
                SEHeaders[pn].extend([PSection + (h,) for h in HPLevel])
            phs = [h for h in phs if h not in HPLevel]
            for si, s in enumerate(LSections[pn]):
                if si + 1 < len(LSections[pn]):
                    SEHeaders[pn].extend(
                        [
                            s + (h,)
                            for h in phs
                            if LSections[pn][si + 1][1].y1
                            < self.get_hy(h, "y0")
                            < s[1].y0
                        ]
                    )
                else:
                    SEHeaders[pn].extend(
                        [s + (h,) for h in phs if self.get_hy(h, "y0") < s[1].y0]
                    )
                PSection = s
        PHeader = None
        for pn, pds in SIdates.items():
            SRows[pn] = []
            if not SEHeaders[pn]:
                SRows[pn] = [PHeader + (pd, 1) for pd in pds]
                continue
            DPLevel = [
                pd for pd in pds if pd.y0 > self.get_hy(SEHeaders[pn][0][2], "y1")
            ]
            if DPLevel:
                SRows[pn].extend([PHeader + (pd, 1) for pd in DPLevel])
            pds = [pd for pd in pds if pd not in DPLevel]
            for hi, h in enumerate(SEHeaders[pn]):
                if hi + 1 < len(SEHeaders[pn]):
                    SRows[pn].extend(
                        [
                            h + (pd, 0)
                            for pd in pds
                            if self.get_hy(SEHeaders[pn][hi + 1][2], "y1")
                            < pd.y0
                            < self.get_hy(h[2], "y0")
                        ]
                    )
                else:
                    SRows[pn].extend(
                        [h + (pd, 0) for pd in pds if pd.y0 < self.get_hy(h[2], "y0")]
                    )
                PHeader = h
        self.cache.summary.rows = SRows
        self.cache.summary.levels = SLevels
        self.cache.summary.topics = SSections
        srn = self.count_pis(self.cache.summary.rows)
        sln = self.count_pis(self.cache.summary.levels)
        stn = self.count_pis(self.cache.summary.topics)
        self.skip_step5 = srn == 0 or sln == 0 or stn == 0
        if srn == 0:
            self.data.error.add("No summary rows found")
        if sln == 0:
            self.data.error.add("No summary levels found")
        if stn == 0:
            self.data.error.add("No summary topics found")
        return self

    def step5(self):
        """
        Extracts information from the financial report and populates the data summary.

        Returns:
            self: The current instance of the class.
        """

        pattern = re.compile("(.*) \((.*)\)")
        for pn, prs in self.cache.summary.rows.items():
            for pr in prs:
                for ct, cd in self.cache.summary.columns[pn].items():
                    rd = {
                        "REPORT_NAME": self.fp.name,
                        "COMPANY_NAME": self.data.company_name,
                        "FISCAL_YEAR": self.data.fiscal_year,
                        "REPORT_DATE_GENERATED": self.data.report_date,
                        "GUIDANCE_LEVEL": pr[0].get_text().strip(),
                        "GUIDANCE_TOPIC": pr[1].get_text().strip(),
                        "GUIDANCE_COMBINED_ITEM": self.get_ht(pr[2]),
                        "GUID_FISCAL_PERIOD": ct,
                    }
                    if match := pattern.match(self.get_ht(pr[2])):
                        rd["GUIDANCE_LINE_ITEM"] = match[1]
                        rd["GUID_INFO"] = match[2]
                    else:
                        rd["GUIDANCE_LINE_ITEM"] = rd["GUIDANCE_COMBINED_ITEM"]
                        rd["GUID_INFO"] = ""
                    db = None
                    if pr[4] == 0:
                        db = next(
                            (
                                b
                                for b in cd
                                if round(b.y0 - self.get_hy(pr[2], "y0"), 2)
                                in [2.16, -1.45, -0.15, 11.76]
                            ),
                            None,
                        )
                    elif pr[4] == 1:
                        db = next(
                            (
                                b
                                for b in self.cache.summary.columns[pn - 1][ct]
                                if round(b.y0 - self.get_hy(pr[2], "y0"), 2)
                                in [2.16, -1.45, -0.15, 11.76]
                            ),
                            None,
                        )

                    if db is not None:
                        rd["GUID_AMT"] = db.get_text().strip()
                        if round(db.y0 - self.get_hy(pr[2], "y0"), 2) == -1.45:
                            sch = next(
                                (b for b in cd if b.get_text().strip() == self.s2),
                                None,
                            )
                            if sch is not None:
                                rd["GUID_AMT"] = f'{rd["GUID_AMT"]}{self.s2}'
                        if rd["GUID_AMT"] == self.s3 or pr[3] is None:
                            rd["GUID_ISSUE_DATE"] = ""
                        else:
                            db = next(
                                (b for b in cd if b.y0 == pr[3].y0),
                                None,
                            )
                            if db is not None:
                                rd["GUID_ISSUE_DATE"] = db.get_text().strip()
                        self.data.summary.append(rd)
        return self

    def report_step1(self):
        """
        Extracts and processes the financial report data from the PDF.

        This method iterates through the pages of the report and performs the following steps:
        1. Sorts the content of each page.
        2. Checks if the page has enough content.
        3. Determines the current period based on the column titles in the cache.
        4. Stores the report pages for each period in a dictionary.

        Returns:
            self: The current instance of the class.

        Raises:
            None

        """
        CPeriod = None
        RPages = {}
        SPage = 0
        for pn, pg in enumerate(self.pages.report):
            bs = self.sorty(self.get_bs(pg))
            if len(bs) <= 1:
                self.data.error.add(f"Check report page {pn}")
                continue
            if pn == 0:
                CPeriod = next(
                    (
                        c["title"]
                        for c in self.cache.summary.column_titles
                        if c["title"] in bs[1].get_text()
                    )
                )
                if pn + 1 == len(self.pages.report):
                    RPages[CPeriod] = self.pages.report[SPage:]
                    RPages[CPeriod] = self.pages.report[SPage:]
                continue
            if pn + 1 == len(self.pages.report):
                RPages[CPeriod] = self.pages.report[SPage:]
                RPages[CPeriod] = self.pages.report[SPage:]
            else:
                pd = next(
                    (
                        c["title"]
                        for c in self.cache.summary.column_titles
                        if c["title"] in bs[0].get_text()
                    ),
                    None,
                )
                if pd is not None:
                    RPages[CPeriod] = self.pages.report[SPage:pn]
                    SPage = pn
                    CPeriod = pd
        self.cache.report.period_reports = RPages
        return self

    def report_step2(self):
        """
        Performs the second step of the financial report extraction process.

        This method extracts headers, issue dates, and contents from the period reports
        stored in the cache. It populates the `headers`, `issue_dates`, and `contents`
        attributes of the cache's report object.

        Returns:
            self: The current instance of the class.
        """
        Headers = {}
        RIDates = {}
        RContents = {}
        for pd, pps in self.cache.report.period_reports.items():
            Headers[pd] = {}
            RIDates[pd] = {}
            RContents[pd] = {}
            PPContent = []
            for pn, pp in enumerate(pps):
                bs = [
                    list(block) if block.get_text().count("\n") > 1 else [block]
                    for block in self.get_bs(pp)
                ]
                bs = self.sorty(list(itertools.chain(*bs)))
                pts = self.sorty(
                    [
                        b
                        for b in bs
                        if b.x0
                        in [
                            self.d05,
                            self.d06,
                        ]
                    ]
                )
                Headers[pd].update({pn: pts})

                PIDates = [b for b in bs if b.x0 == self.d07]
                Headers[pd][pn].extend(PIDates)
                RIDates[pd].update({pn: PIDates})

                pcs = [
                    list(b) if self.is_mb(b) else [b]
                    for b in self.get_bs(pp)
                    if b.x0 == self.d08
                ]
                pcs = list(itertools.chain(*pcs))
                if PPContent:
                    pcs = self.sorty(PPContent) + self.sorty(pcs)
                    PPContent = []
                RContents[pd].update({pn: pcs})

                PSTypes = self.sorty([b for b in bs if b.x1 == self.d09])
                if PSTypes and PSTypes[-1].get_text().count("\n") > 1:
                    PPContent = [
                        list(b) if b.get_text().count("\n") > 1 else [b]
                        for b in list(PSTypes[-1])[1:]
                    ]
                    PPContent = self.sorty(list(itertools.chain(*PPContent)))
                    PSTypes = PSTypes[:-1] + [list(PSTypes[-1])[0]]
                Headers[pd][pn].extend(self.sorty(PSTypes))
                Headers[pd][pn] = self.sorty(Headers[pd][pn])
        self.cache.report.headers = Headers
        self.cache.report.issue_dates = RIDates
        self.cache.report.contents = RContents
        return self

    def report_step3(self):
        """
        This method processes the headers of the financial report and organizes them into levels, topics, items, and sections.

        Returns:
            self: The current instance of the class.
        """

        RLevels = {}
        RTopics = {}
        RItems = {}
        RSections = {}
        for pd, pps in self.cache.report.headers.items():
            PLevels = {}
            PTopics = {}
            PItems = {}
            PSections = {}
            PLevel = None
            PTopic = None
            PDate = None
            PItem = None
            for pn, pe in pps.items():
                PGLevels = []
                PGItems = []
                PGTopics = []
                PGSections = []
                PGSection = ()
                for i, it in enumerate(pe):
                    if it.x1 == self.d09:
                        PGSection = PGSection + (it,)
                        if len(PGSection) == 1:
                            PGSection = (
                                PLevel,
                                PTopic,
                                PItem,
                                PDate,
                            ) + PGSection
                        elif len(PGSection) == 2:
                            PGSection = (
                                PLevel,
                                PTopic,
                                PItem,
                            ) + PGSection
                        elif len(PGSection) == 3:
                            PGSection = (PLevel, PTopic) + PGSection
                        elif len(PGSection) == 4:
                            PGSection = (PLevel,) + PGSection
                        PGSections.append(PGSection)
                        PGSection = ()
                        continue
                    if it.x0 == self.d07:
                        PGSection = PGSection + (it,) if "/" in it.get_text() else ()
                        PDate = it
                        continue
                    if it.x0 != self.d04:
                        PGItems.append(it)
                        PItem = it
                        PGSection = PGSection + (it,)
                        continue
                    if (
                        i + 1 < len(pe)
                        and pe[i + 1].x0 == self.d05
                        and round(it.y0 - pe[i + 1].y0, 2) == self.d14
                    ):
                        PGLevels.append(it)
                        PLevel = it
                    elif (
                        i + 1 == len(pe)
                        and it.y0 == self.d11
                        and pn + 1 in pps
                        and pps[pn + 1][0].y0 == self.d10
                        and pps[pn + 1][0].x0 == self.d05
                    ):
                        PGLevels.append(it)
                        PLevel = it
                    else:
                        PGTopics.append(it)
                        PTopic = it
                    PGSection = PGSection + (it,)
                PLevels[pn] = PGLevels
                PTopics[pn] = PGTopics
                PItems[pn] = PGItems
                PSections[pn] = PGSections
            RLevels[pd] = PLevels
            RTopics[pd] = PTopics
            RItems[pd] = PItems
            RSections[pd] = PSections
        self.cache.report.levels = RLevels
        self.cache.report.topics = RTopics
        self.cache.report.report_items = RItems
        self.cache.report.sections = RSections
        return self

    def report_step4(self):
        """
        Performs step 4 of the financial report extraction process.

        This method updates the report sections and contents based on the provided cache.
        It iterates through each page and section in the cache, and for each section,
        it finds the corresponding source in the report contents and removes it.
        It then looks for additional sources below the current source and removes them as well.
        The updated sections and contents are stored in the cache.

        Returns:
            self: The current instance of the class.
        """

        RSections = {}
        RContents = deepcopy(self.cache.report.contents)
        for pd, pps in self.cache.report.sections.items():
            RSections[pd] = {}
            for pn, pss in pps.items():
                CSections = []
                for s in pss:
                    src = next(
                        (
                            b
                            for b in RContents[pd][pn]
                            if b.y0 == s[4].y0
                            # and b.y1 == s[4].y1
                        ),
                        None,
                    )
                    if src is not None:
                        RContents[pd][pn] = [b for b in RContents[pd][pn] if b != src]
                    sources = [src]
                    while src is not None:
                        src = next(
                            (
                                b
                                for b in RContents[pd][pn]
                                if round(b.y0, 2) == round(src.y0 + 12, 2)
                            ),
                            None,
                        )
                        if src is not None:
                            RContents[pd][pn] = self.sorty(
                                [b for b in RContents[pd][pn] if b != src]
                            )
                            sources = [src] + sources

                    CSections.append((pd, pn) + s + (sources,))
                RSections[pd][pn] = CSections
        self.cache.report.updated_sections = RSections
        self.cache.report.updated_contents = RContents
        return self

    def report_step5(self):
        """
        Extracts and organizes the headers from the period reports in the cache.

        Returns:
            self: The current instance of the class.
        """

        GHeaders = {}
        for pd, pps in self.cache.report.period_reports.items():
            GHeaders[pd] = {}
            for pn, pp in enumerate(pps):
                bs = self.sorty(
                    [
                        b
                        for b in self.get_bs(pp)
                        if b.x0 < self.d08
                        and b.x0
                        not in [
                            self.d04,
                            self.d05,
                            self.d06,
                            self.d07,
                        ]
                    ]
                )
                CBlocks = []
                for b in bs:
                    if not CBlocks:
                        CBlocks.append([b])
                    elif abs(round(b.y0 - CBlocks[-1][-1].y0, 0)) <= self.d13:
                        CBlocks[-1].append(b)
                    else:
                        CBlocks.append([b])
                GHeaders[pd][pn] = CBlocks
        self.cache.report.guid_headers = GHeaders
        return self

    def report_step6(self):
        """
        Updates the report sections by merging headers with the same y-coordinate.

        Returns:
            self: The updated object.
        """
        RSections = deepcopy(self.cache.report.updated_sections)
        for pd, pps in self.cache.report.guid_headers.items():
            for pn, pgh in pps.items():
                for gh in pgh:
                    if not RSections[pd][pn]:
                        ngb = 1
                        with contextlib.suppress(Exception):
                            while True:
                                if RSections[pd][pn - ngb]:
                                    break
                                else:
                                    ngb += 1
                            RSections[pd][pn - ngb][-1] = RSections[pd][pn][-ngb] + (
                                gh,
                            )
                        continue
                    lsid = None
                    for si, s in enumerate(RSections[pd][pn]):
                        if len(s) == 9:
                            continue
                        if abs(s[7][0].y0 - gh[0].y0) < 17:
                            lsid = si
                    if lsid is not None:
                        RSections[pd][pn][lsid] = RSections[pd][pn][lsid] + (gh,)
                    else:
                        ngb = 1
                        with contextlib.suppress(Exception):
                            while True:
                                if RSections[pd][pn - ngb]:
                                    break
                                else:
                                    ngb += 1
                            RSections[pd][pn - ngb][-1] = RSections[pd][pn][-ngb] + (
                                gh,
                            )
        for pd in list(RSections):
            for pn in list(RSections[pd]):
                for si, s in enumerate(RSections[pd][pn]):
                    if len(s) == 8:
                        RSections[pd][pn][si] = RSections[pd][pn][si] + (
                            RSections[pd][pn][si - 1][-1],
                        )
        self.cache.report.guid_sections = RSections
        return self

    def report_step7(self):
        """
        Extracts and organizes sections from a financial report.

        This method iterates over the sections of a financial report and organizes them into a structured format.
        It uses the cache of the report's contents and sections to perform the extraction.

        Returns:
            self: The current instance of the class.

        Raises:
            IndexError: If there is an error on a specific report page.

        """
        RSectioned = []
        for pd, pps in self.cache.report.guid_sections.items():
            for pn, pss in pps.items():
                cuis = []
                if pss:
                    if cbp := [
                        ci
                        for ci in self.cache.report.contents[pd][pn]
                        if ci.y0 > pss[0][6].y1
                        and ci.y0 - pss[0][6].y1 >= 12
                        and not self.is_gray(ci)
                    ]:
                        try:
                            RSectioned[-1][6].extend(cbp)
                            cuis.extend(cbp)
                        except IndexError:
                            self.data.error.add(f"Error on report page {pn} of {pd}")
                elif RSectioned:
                    RSectioned[-1][6].extend(self.cache.report.contents[pd][pn])
                    cuis.extend(self.cache.report.contents[pd][pn])
                for si, ps in enumerate(pss):
                    cis = []
                    for ci in self.cache.report.contents[pd][pn]:
                        if si + 1 < len(pss):
                            if (
                                pss[si + 1][6].y1 < ci.y0 < ps[6].y0
                            ) and ci not in cuis:
                                cis.append(ci)
                                cuis.append(ci)
                        elif ci.y0 < ps[6].y0:
                            if ci not in cuis:
                                cis.append(ci)
                                cuis.append(ci)
                    section_content = ps + (cis,)
                    RSectioned.append(section_content)
        self.cache.report.sectioned_report = RSectioned
        return self

    def report_step8(self):
        """
        Extracts information from the sectioned report and populates the `self.data.report` attribute.

        Returns:
            self: The current instance of the class.
        """
        RData = []
        pattern_it = re.compile(".*issued on (.*)")
        pattern_src = re.compile("([^,]+),(.*): *(“.*)")
        pattern_title = re.compile("(.*): *(“.*)")
        for s in self.cache.report.sectioned_report:
            stbs = list(s[6])
            sd = {
                "REPORT_NAME": self.fp.name,
                "GUIDANCE_LEVEL": s[2].get_text().strip(),
                "GUIDANCE_TOPIC": s[3].get_text().strip(),
                "GUIDANCE_LINE_ITEM": s[4].get_text().strip(),
                "GUID_FISCAL_PERIOD": s[0],
                "SOURCE": self.get_st(s[7]),
                "SOURCE_TYPE": stbs[0].get_text().strip(),
                "GUID_AMT": (
                    s[8][0].get_text().strip()
                    if len(s[8]) in {1, 2}
                    else (
                        f"{s[8][1].get_text().strip()}{s[8][0].get_text().strip()}"
                        if len(s[8]) == 3
                        else ""
                    )
                ),
                "GUID_INFO": (s[8][-1].get_text().strip() if len(s[8]) > 1 else ""),
            }
            contents = self.sorty(s[9]) + stbs[1:]
            if mtm := pattern_it.match(s[5].get_text().strip()):
                sd["LAST_ISSUE_DATETIME"] = mtm[1]
            PContents = []
            fis = None
            for c in contents:
                ct = c.get_text().strip()
                if fis is not None:
                    ct = f"{fis} {ct}"
                if mtl := pattern_title.match(ct):
                    if ms := pattern_src.match(ct):
                        PContents.append(
                            {
                                "SOURCE_PERSON_NAME": ms[1],
                                "SOURCE_PERSON_TITLE": ms[2],
                                "TEXT": ms[3],
                            }
                        )
                    else:
                        PContents.append(
                            {
                                "SOURCE_PERSON_TITLE": mtl[1],
                                "TEXT": mtl[2],
                            }
                        )
                    fis = None
                elif (
                    isinstance(c, LTTextBoxHorizontal)
                    and all(
                        it.graphicstate.scolor == self.c1
                        for l in c
                        for it in l
                        if isinstance(it, LTChar)
                    )
                ) or (
                    isinstance(c, LTTextLineHorizontal)
                    and all(
                        it.graphicstate.scolor == self.c1
                        for it in c
                        if isinstance(it, LTChar)
                    )
                ):
                    fis = c.get_text().strip()
                elif len(PContents) > 0:
                    PContents[-1]["TEXT"] = f'{PContents[-1]["TEXT"]} {ct}'
                else:
                    PContents.append(
                        {
                            "TEXT": ct,
                        }
                    )
            for pc in PContents:
                pc["TEXT"] = pc["TEXT"].strip()
                RData.append({**sd, **pc})
        self.data.report = RData
        return self

    @staticmethod
    def sorty(bs):
        return sorted(bs, key=lambda b: -b.y0)

    def get_bs(self, p):
        return [
            b
            for b in list(p)
            if isinstance(b, (LTTextBoxHorizontal, LTTextLineHorizontal))
            and self.d15 > b.y0 > self.d16
        ]

    def rm_id(self, bs):
        return [b for b in bs if b.x0 != self.d03], [b for b in bs if b.x0 == self.d03]

    @staticmethod
    def get_hy(data, at):
        if isinstance(data, list):
            return getattr(data[0], at)
        return getattr(data, at)

    @staticmethod
    def get_ht(data):
        if isinstance(data, list):
            return " ".join([h.get_text().strip() for h in data])
        return data.get_text().strip()

    @staticmethod
    def is_mb(data: LTTextLineHorizontal | LTTextBoxHorizontal):
        return data.get_text().count("\n") > 1

    @staticmethod
    def count_pis(data):
        return sum(len(p) for p in data.values())

    @staticmethod
    def get_st(data):
        if any(p is None for p in data):
            return "ERROR"
        return " ".join([p.get_text().strip() for p in data])

    def is_gray(self, data: LTTextBoxHorizontal | LTTextLineHorizontal):
        return (
            isinstance(data, LTTextBoxHorizontal)
            and all(
                it.graphicstate.scolor == self.c1
                for l in data
                for it in l
                if isinstance(it, LTChar)
            )
        ) or (
            isinstance(data, LTTextLineHorizontal)
            and all(
                it.graphicstate.scolor == self.c1
                for it in data
                if isinstance(it, LTChar)
            )
        )


class ReportBatchReader:
    def __init__(self, ip: Path | str, op: Path | str):
        self.ip = Path(ip)
        if not self.ip.exists():
            raise FileNotFoundError(f"Input path {self.ip} does not exist")
        self.op = Path(op)
        self.summaries = []
        self.reports = []
        self.errors = []
        self.no_data = True

    def get_files(self):
        """
        Retrieves a list of PDF files from the specified directory and its subdirectories.

        Returns:
            self: The current instance of the class.
        """
        for f in self.ip.glob("**/*.pdf"):
            f.rename(f.with_name(f"{f.stem.replace('Guidance Summary ', '')}.pdf"))
        self.files = list(self.ip.glob("**/*.pdf"))
        return self

    def get_data(self):
        """
        Retrieves data from the files and stores it in the object.

        Returns:
            self: The current object with the retrieved data.
        """

        self.no_data = len(self.files) == 0
        if self.no_data:
            return self
        for f in tqdm(self.files, desc="Processing files"):
            r = ReportReader(f, nocaching=True)
            r.read()
            summary, report, error = r.summary, r.report, r.error
            if summary is not None:
                self.summaries.append(summary)
            if report is not None:
                self.reports.append(report)
            if error:
                self.errors.extend(error)
        return self

    def export(self):
        """
        Export the data to Excel files.

        This method exports the data to multiple Excel files based on the starting character of the report name.
        It creates separate sheets for the summary and report data in each Excel file.

        Returns:
            self: The current instance of the class.

        """
        if self.no_data:
            return self
        self.op.mkdir(parents=True, exist_ok=True)
        with pandas.ExcelWriter(self.op / "log.xlsx") as writer:
            self.error.to_excel(writer, index=False)
        for c in tqdm(ascii_uppercase + digits, desc="Exporting files"):
            summary = self.summary[self.summary["REPORT_NAME"].str.startswith(c)]
            summary = summary[
                [
                    "REPORT_NAME",
                    "COMPANY_NAME",
                    "FISCAL_YEAR",
                    "REPORT_DATE_GENERATED",
                    "GUIDANCE_LEVEL",
                    "GUIDANCE_TOPIC",
                    "GUIDANCE_COMBINED_ITEM",
                    "GUIDANCE_LINE_ITEM",
                    "GUID_INFO",
                    "GUID_FISCAL_PERIOD",
                    "GUID_ISSUE_DATE",
                    "GUID_AMT",
                ]
            ]
            report = self.report[self.report["REPORT_NAME"].str.startswith(c)]
            if summary.shape[0] == 0 and report.shape[0] == 0:
                continue
            summary = summary.drop_duplicates()
            report = report.drop_duplicates()
            with pandas.ExcelWriter(self.op / f"Guidance_DataSheet_{c}.xlsx") as writer:
                summary.to_excel(writer, sheet_name="GUIDANCE_HEADER", index=False)
                report.to_excel(
                    writer, sheet_name="GUIDANCE_SOURCE_DETAIL", index=False
                )

        for c in tqdm(ascii_uppercase + digits, desc="Merging files"):
            if self.op.joinpath(f"/Guidance_DataSheet_{c}.xlsx").exists():
                df = pandas.read_excel(
                    self.op / f"Guidance_DataSheet_{c}.xlsx",
                    sheet_name="GUIDANCE_SOURCE_DETAIL",
                )
                df = df.reset_index()
                df = df.rename(columns={"index": "SEQUENCE_ID"})
                df = df[
                    [
                        "REPORT_NAME",
                        "GUIDANCE_LEVEL",
                        "GUIDANCE_TOPIC",
                        "GUIDANCE_LINE_ITEM",
                        "GUID_FISCAL_PERIOD",
                        "GUID_AMT",
                        "GUID_INFO",
                        "SEQUENCE_ID",
                        "LAST_ISSUE_DATETIME",
                        "SOURCE",
                        "SOURCE_TYPE",
                        "SOURCE_PERSON_NAME",
                        "SOURCE_PERSON_TITLE",
                        "TEXT",
                    ]
                ]
                df["SEQUENCE_ID"] = df["SEQUENCE_ID"] + 1
                with pandas.ExcelWriter(
                    self.op / f"Guidance_DataSheet_{c}.xlsx",
                    mode="a",
                    if_sheet_exists="replace",
                ) as writer:
                    df.to_excel(
                        writer, sheet_name="GUIDANCE_SOURCE_DETAIL", index=False
                    )

    @property
    def summary(self):
        return pandas.concat(self.summaries, ignore_index=True)

    @property
    def report(self):
        return pandas.concat(self.reports, ignore_index=True)

    @property
    def error(self):
        return pandas.DataFrame(self.errors)


if __name__ == "__main__":
    input_path = "INSERT/INPUT/PATH"
    output_path = "INSERT/OUTPUT/PATH"
    Path(output_path).mkdir(parents=True, exist_ok=True)
    ReportBatchReader(input_path, output_path).get_files().get_data().export()
