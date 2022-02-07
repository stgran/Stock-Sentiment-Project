# Stock Sentiment Project
This project is based on the idea that Tweets about companies will be related, and may precede, to the stock price of that company.

The hypothesis is two-fold:
1. The Tweets and stock price share a common cause: the state of the company. If Macbooks are exploding all over the world, Apple stock will tank because people believe it has released a product that will lose them money, and people will trash Apple on Twitter (and elsewhere) because of the terrible product.
    - This hypothesis suggests that my Twitter-stock market strategy will lend itself more to consumer-/user-facing companies such as Apple, Starbucks, Walmart, etc. rather than large industrial, real-estate, or business-facing companies. This is because, for consumer-/user-facing companies, there exist more users and consumers to discuss that company on Twitter.
    - Additionally, consumer-/user-facing companies will likely be easier to search on Twitter because they need unique, memorable names. Industrial, real-estate, and business-facing companies are less likely to have interesting or unique names and will therefore be harder to search on Twitter, since a name like "American Water" could return a huge variety of results unrelated to the company with that name.
2. Tweets may, to a small extent, drive stock price. If people see a ton of negative Tweets about Apple, they may decide to sell stock. If people see positive Tweets, they may choose to buy stock.
    - If the above hypothesis that consumer-/user-facing companies will have more discussion on Twitter because of the larger population that is familiar with or uses the company's product or platform, this hypothesis also suggests that the Twitter-stock market strategy will lend itself more to those consumer-/user-facing companies.
    - At the moment, these Twitter-scraping script records the sentiment and polarity of the texts of Tweets scraped. As data gets collected, I am going to work on a stock-specific sentiment analyzer that focuses on terms related to buying and selling. This analyzer would not search for conventional positive or negative language but instead stock-focused language.

To test these hypotheses, I intend to record the price and Tweets for a variety of stocks, whose selection I will discuss later in this ReadMe. I will do the same for a few cryptocurrencies due to my own curiosity.

At the moment, this project consists only of web-scraping files which I am running on my Raspberry Pi to gather enough data to test the above hypothesis.

Once I have enough Twitter and stock data, I will choose a model with which to explore the relationship between the sentiment of Tweets about a company or cryptocurrency and that  stock or cryptocurrency's price over time.

## API Limits
### Twitter API
I have the "Elevated" level of the Twitter API.
Tweet caps: Retrieve up to 2m Tweets per month

My Twitter scraper makes a maximum of fifteen requests totalling 1500 tweets in a single run. The 2m monthly max gives me at least 1333 runs per month. That allows me to run once per day for at least ~44 stocks. However, for many stocks for which I will search, it is highly unlikely that my code will return 1500 new tweets every day, which gives me a bit more flexibility.

### AlphaVantage API
AlphaVantage's call limit is up to 5 API requests per minute and 500 requests per day. Given the Twitter API limitations, this should not impact this process. This just means that I need to spread out my requests.

## Stocks and Cryptocurrencies
I have chosen to collect data for a variety of stocks and cryptocurrencies.

### Cryptocurrencies
Because cryptocurrencies have no intrinsic value in the way that a company might, their prices should be determined purely by investor feeling and perspective (as well as the available capital for investment). This serves as an interesting control-ish variable.

|Symbol|Cryptocurrency            |Query terms           |
|------|--------------------------|----------------------|
|BTC   |Bitcoin                   |bitcoin, btc, $btc    |
|ETH   |Ethereum                  |ethereum, eth, $eth   |
|DOT   |Polkadot                  |polkadot, $dot        |

### Stocks
For stocks, I have chosen to focus on S&P 500 stocks. This will likely limit the applicability of any findings that emerge from this project but that is acceptable. Sticking with the S&P 500 should also be beneficial because it should be easier to find Tweets about these larger companies than it would for a random unknown stock.

Because I am also collecting Tweets for three cryptocurrencies, I have at least 41 remaining stocks. However, I am willing to bet that queries for many stocks will consistently return fewer than 1500 tweets per run, so I will select 50 S&P 500 stocks. These stocks will be chosen randomly.

#### 50 random S&P 500 Stocks
The fifty stocks listed below were part of the S&P 500 as of February 6, 2022. Even if one of these stocks drop out of the S&P 500, it will remain in this list.
|Symbol|Security                 |GICS Sector           |Query Terms              |
|------|-------------------------|----------------------|-------------------------|
|AWK   |American Water           |Utilities             |american water, $awk     |
|NTRS  |Northern Trust           |Financials            |northern trust, $ntrs|
|PLD   |Prologis                 |Real Estate           |prologis, $pld      |
|BSX   |Boston Scientific        |Health Care           |boston scientific, $bsx|
|ROK   |Rockwell Automation      |Industrials           |rockwell automation, $rok|
|IR    |Ingersoll Rand           |Industrials           |ingersoll rand, $ir      |
|EQR   |Equity Residential       |Real Estate           |equity residential, $eqr|
|PAYC  |Paycom                   |Information Technology|paycom, $payc      |
|QRVO  |Qorvo                    |Information Technology|qorvo, $qrvo       |
|AMGN  |Amgen                    |Health Care           |amgen, $amgn       |
|NEM   |Newmont                  |Materials             |newmont, $nem       |
|TGT   |Target                   |Consumer Discretionary|target, $tgt        |
|BDX   |Becton Dickinson         |Health Care           |becton dickinson, $bdx|
|CDAY  |Ceridian                 |Information Technology|ceridian, $cday    |
|LRCX  |Lam Research             |Information Technology|lam research, $lrcx|
|MHK   |Mohawk Industries        |Consumer Discretionary|mohawk industries, $mhk|
|PSX   |Phillips 66              |Energy                |phillips 66, $psx   |
|PFE   |Pfizer                   |Health Care           |pfizer, $pfe        |
|IEX   |IDEX Corporation         |Industrials           |idex, $iex          |
|ORCL  |Oracle                   |Information Technology|oracle, $orcl      |
|BLL   |Ball                     |Materials             |ball, $bll          |
|FDS   |FactSet                  |Financials            |factset, $fds       |
|KMI   |Kinder Morgan            |Energy                |kinder morgan, $kmi |
|ETR   |Entergy                  |Utilities             |entergy, $etr            |
|OTIS  |Otis Worldwide           |Industrials           |otis worldwide, $otis    |
|NWSA  |News Corp (Class A)      |Communication Services|news corp, $nwsa |
|FAST  |Fastenal                 |Industrials           |fastenal, $fast  |
|MAR   |Marriott International   |Consumer Discretionary|marriott, $mar   |
|TMO   |Thermo Fisher Scientific |Health Care           |thermo fisher, $tmo |
|CAT   |Caterpillar              |Industrials           |caterpillar, $cat   |
|IPG   |Interpublic Group        |Communication Services|interpublic, $ipg |
|AAPL  |Apple                    |Information Technology|apple, $aapl, iphone, macbook |
|XYL   |Xylem                    |Industrials           |xylem, $xyl |
|O     |Realty Income Corporation|Real Estate           |realty income, $o |
|HPQ   |HP                       |Information Technology|hp, $hpq |
|KEY   |KeyCorp                  |Financials            |keycorp, $key |
|BEN   |Franklin Resources       |Financials            |franklin resources, $ben |
|IDXX  |Idexx Laboratories       |Health Care           |idexx, $idxx |
|NDAQ  |Nasdaq                   |Financials            |nasdaq, $ndaq |
|WELL  |Welltower                |Real Estate           |welltower, $well |
|ALL   |Allstate                 |Financials            |allstate, $all |
|ATO   |Atmos Energy             |Utilities             |atmos, $ato |
|ALK   |Alaska Air Group         |Industrials           |alaska airlines, horizon airlines, $alk |
|SYK   |Stryker Corporation      |Health Care           |stryker, $syk |
|CRL   |Charles River            |Health Care           |charles river, $crl |
|VFC   |VF Corporation           |Consumer Discretionary|vf corporation, $vfc |
|AAP   |Advance Auto Parts       |Consumer Discretionary|advance auto parts, $aap |
|DG    |Dollar General           |Consumer Discretionary|dollar general, $dg |
|MLM   |Martin Marietta Materials|Materials             |martin marietta, $mlm |
|GPS   |Gap                      |Consumer Discretionary|gap, $gps |

